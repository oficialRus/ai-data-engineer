package usecase

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/user/ai-data-engineer/backend/internal/config"
	infra "github.com/user/ai-data-engineer/backend/internal/infra/ws"
)

type AnalyzeService struct {
	Hub *infra.Hub
	Cfg config.AppConfig
}

type PipelineService struct {
	Hub *infra.Hub
	Cfg config.AppConfig
}

func (s *AnalyzeService) StartAnalyze(preview json.RawMessage) (string, error) {
	id := uuid.NewString()
	go s.runAnalyze(id, preview)
	return id, nil
}

func (s *PipelineService) CreatePipeline(req json.RawMessage) (string, error) {
	id := uuid.NewString()
	go s.runPipeline(id, req)
	return id, nil
}

func (s *AnalyzeService) runAnalyze(id string, preview json.RawMessage) {
	scope := "analyze"
	s.Hub.Broadcast(scope, id, map[string]any{"type": "queued", "data": map[string]any{"id": id, "scope": scope}})
	time.Sleep(200 * time.Millisecond)
	s.Hub.Broadcast(scope, id, map[string]any{"type": "started", "data": map[string]any{"id": id, "scope": scope}})
	for p := 0; p <= 100; p += 25 {
		stage := "extract"
		if p >= 25 {
			stage = "transform"
		}
		if p >= 50 {
			stage = "load"
		}
		if p >= 75 {
			stage = "validate"
		}
		msg := fmt.Sprintf("analyze %s %d%%", stage, p)
		s.Hub.Broadcast(scope, id, map[string]any{"type": "progress", "data": map[string]any{"id": id, "scope": scope, "percent": p, "stage": stage, "message": msg}})
		s.Hub.Broadcast(scope, id, map[string]any{"type": "log", "data": map[string]any{"id": id, "scope": scope, "level": "info", "line": msg}})
		time.Sleep(200 * time.Millisecond)
	}
	// Реальный вызов ML с отладкой
	client := &http.Client{Timeout: time.Duration(s.Cfg.MLTimeoutSec) * time.Second}
	url := s.Cfg.MLBaseURL + s.Cfg.MLAnalyzePath
	reqBody := map[string]any{"id": id}
	if len(preview) > 0 {
		var p map[string]any
		_ = json.Unmarshal(preview, &p)
		reqBody["preview"] = p
	}
	bodyBytes, _ := json.Marshal(reqBody)
	log.Printf("[ANALYZE] Request to ML: %s", url)
	log.Printf("[ANALYZE] Request body: %s", string(bodyBytes))

	resp, err := client.Post(url, "application/json", bytes.NewReader(bodyBytes))
	if err != nil {
		log.Printf("[ANALYZE] ML request failed: %v", err)
		s.Hub.Broadcast(scope, id, map[string]any{"type": "error", "data": map[string]any{"id": id, "scope": scope, "reason": err.Error()}})
		return
	}
	defer resp.Body.Close()

	log.Printf("[ANALYZE] ML response status: %d", resp.StatusCode)
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[ANALYZE] Failed to read ML response: %v", err)
		s.Hub.Broadcast(scope, id, map[string]any{"type": "error", "data": map[string]any{"id": id, "scope": scope, "reason": err.Error()}})
		return
	}
	log.Printf("[ANALYZE] ML response body: %s", string(respBody))

	if resp.StatusCode >= 400 {
		s.Hub.Broadcast(scope, id, map[string]any{"type": "error", "data": map[string]any{"id": id, "scope": scope, "reason": fmt.Sprintf("ML returned %d: %s", resp.StatusCode, string(respBody))}})
		return
	}

	var mlResp map[string]any
	if err := json.Unmarshal(respBody, &mlResp); err != nil {
		log.Printf("[ANALYZE] Failed to parse ML response: %v", err)
		s.Hub.Broadcast(scope, id, map[string]any{"type": "error", "data": map[string]any{"id": id, "scope": scope, "reason": err.Error()}})
		return
	}
	log.Printf("[ANALYZE] Parsed ML response: %+v", mlResp)
	s.Hub.Broadcast(scope, id, map[string]any{"type": "done", "data": map[string]any{"id": id, "scope": scope, "payload": mlResp}})
}

func (s *PipelineService) runPipeline(id string, req json.RawMessage) {
	scope := "pipeline"
	s.Hub.Broadcast(scope, id, map[string]any{"type": "queued", "data": map[string]any{"id": id, "scope": scope}})
	s.Hub.Broadcast(scope, id, map[string]any{"type": "started", "data": map[string]any{"id": id, "scope": scope}})
	client := &http.Client{Timeout: time.Duration(s.Cfg.MLTimeoutSec) * time.Second}
	url := s.Cfg.MLBaseURL + s.Cfg.MLPipelinePath
	// Пробрасываем исходный запрос в ML, добавив id
	var body map[string]any
	if len(req) > 0 {
		_ = json.Unmarshal(req, &body)
	} else {
		body = map[string]any{}
	}
	body["id"] = id
	bodyBytes, _ := json.Marshal(body)
	log.Printf("[PIPELINE] Request to ML: %s", url)
	log.Printf("[PIPELINE] Request body: %s", string(bodyBytes))

	resp, err := client.Post(url, "application/json", bytes.NewReader(bodyBytes))
	if err != nil {
		log.Printf("[PIPELINE] ML request failed: %v", err)
		s.Hub.Broadcast(scope, id, map[string]any{"type": "error", "data": map[string]any{"id": id, "scope": scope, "reason": err.Error()}})
		return
	}
	defer resp.Body.Close()

	log.Printf("[PIPELINE] ML response status: %d", resp.StatusCode)
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[PIPELINE] Failed to read ML response: %v", err)
		s.Hub.Broadcast(scope, id, map[string]any{"type": "error", "data": map[string]any{"id": id, "scope": scope, "reason": err.Error()}})
		return
	}
	log.Printf("[PIPELINE] ML response body: %s", string(respBody))

	if resp.StatusCode >= 400 {
		s.Hub.Broadcast(scope, id, map[string]any{"type": "error", "data": map[string]any{"id": id, "scope": scope, "reason": fmt.Sprintf("ML returned %d: %s", resp.StatusCode, string(respBody))}})
		return
	}

	var mlResp map[string]any
	if err := json.Unmarshal(respBody, &mlResp); err != nil {
		log.Printf("[PIPELINE] Failed to parse ML response: %v", err)
		s.Hub.Broadcast(scope, id, map[string]any{"type": "error", "data": map[string]any{"id": id, "scope": scope, "reason": err.Error()}})
		return
	}
	log.Printf("[PIPELINE] Parsed ML response: %+v", mlResp)
	s.Hub.Broadcast(scope, id, map[string]any{"type": "done", "data": map[string]any{"id": id, "scope": scope, "payload": mlResp}})
}
