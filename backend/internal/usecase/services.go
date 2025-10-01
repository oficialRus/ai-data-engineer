package usecase

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
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
	// Реальный вызов ML: формируем multipart/form-data с файлом CSV из preview
	client := &http.Client{Timeout: time.Duration(s.Cfg.MLTimeoutSec) * time.Second}
	url := s.Cfg.MLBaseURL + s.Cfg.MLAnalyzePath

	csvBytes, headers, rowsCount, err := buildCSVFromPreview(preview)
	if err != nil {
		log.Printf("[ANALYZE] Failed to build CSV from preview: %v", err)
		s.Hub.Broadcast(scope, id, map[string]any{"type": "error", "data": map[string]any{"id": id, "scope": scope, "reason": err.Error()}})
		return
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	// Добавляем файл
	filePart, err := writer.CreateFormFile("file", "preview.csv")
	if err != nil {
		log.Printf("[ANALYZE] Failed to create form file: %v", err)
		s.Hub.Broadcast(scope, id, map[string]any{"type": "error", "data": map[string]any{"id": id, "scope": scope, "reason": err.Error()}})
		return
	}
	if _, err := filePart.Write(csvBytes); err != nil {
		log.Printf("[ANALYZE] Failed to write CSV to form file: %v", err)
		s.Hub.Broadcast(scope, id, map[string]any{"type": "error", "data": map[string]any{"id": id, "scope": scope, "reason": err.Error()}})
		return
	}
	// Дополнительные поля для ML
	_ = writer.WriteField("fileType", "csv")
	_ = writer.WriteField("id", id)
	// Для отладки можно передать метаданные
	_ = writer.WriteField("columns", fmt.Sprintf("%v", headers))
	_ = writer.WriteField("rowCount", fmt.Sprintf("%d", rowsCount))
	if err := writer.Close(); err != nil {
		log.Printf("[ANALYZE] Failed to close multipart writer: %v", err)
		s.Hub.Broadcast(scope, id, map[string]any{"type": "error", "data": map[string]any{"id": id, "scope": scope, "reason": err.Error()}})
		return
	}

	contentType := writer.FormDataContentType()
	log.Printf("[ANALYZE] Request to ML (multipart): %s, csvBytes=%d, headers=%d, rows=%d", url, len(csvBytes), len(headers), rowsCount)

	resp, err := client.Post(url, contentType, bytes.NewReader(body.Bytes()))
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

// buildCSVFromPreview строит CSV в памяти из JSON preview.
// Ожидается структура: { "columns": [{"name":"..."}, ...], "rows": [ {col: val, ...}, ... ] }
func buildCSVFromPreview(preview json.RawMessage) ([]byte, []string, int, error) {
	type column struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	type previewPayload struct {
		Columns []column         `json:"columns"`
		Rows    []map[string]any `json:"rows"`
	}

	var p previewPayload
	if err := json.Unmarshal(preview, &p); err != nil {
		return nil, nil, 0, fmt.Errorf("invalid preview: %w", err)
	}
	if len(p.Columns) == 0 {
		return nil, nil, 0, fmt.Errorf("preview has no columns")
	}

	headers := make([]string, len(p.Columns))
	for i, c := range p.Columns {
		headers[i] = c.Name
	}

	buf := &bytes.Buffer{}
	w := csv.NewWriter(buf)
	if err := w.Write(headers); err != nil {
		return nil, nil, 0, fmt.Errorf("failed to write headers: %w", err)
	}
	rowsWritten := 0
	for _, row := range p.Rows {
		rec := make([]string, len(headers))
		for i, h := range headers {
			if v, ok := row[h]; ok && v != nil {
				rec[i] = fmt.Sprint(v)
			} else {
				rec[i] = ""
			}
		}
		if err := w.Write(rec); err != nil {
			return nil, nil, 0, fmt.Errorf("failed to write row: %w", err)
		}
		rowsWritten++
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return nil, nil, 0, fmt.Errorf("csv writer error: %w", err)
	}
	return buf.Bytes(), headers, rowsWritten, nil
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
