package usecase

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	infra "github.com/user/ai-data-engineer/backend/internal/infra/ws"
)

type AnalyzeService struct {
	Hub *infra.Hub
}

type PipelineService struct {
	Hub *infra.Hub
}

func (s *AnalyzeService) StartAnalyze(preview json.RawMessage) (string, error) {
	id := uuid.NewString()
	go s.simulateAnalyze(id)
	return id, nil
}

func (s *PipelineService) CreatePipeline(req json.RawMessage) (string, error) {
	id := uuid.NewString()
	go s.simulatePipeline(id)
	return id, nil
}

func (s *AnalyzeService) simulateAnalyze(id string) {
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
	payload := map[string]any{
		"recommendation": map[string]any{
			"target":        "clickhouse",
			"confidence":    0.85,
			"rationale":     "Числовые метрики и агрегации подходят для ClickHouse",
			"schedule_hint": "*/30 * * * *",
		},
		"ddl": map[string]any{
			"clickhouse": "CREATE TABLE ... ENGINE = MergeTree",
			"postgresql": "CREATE TABLE ...",
			"hdfs":       "PARQUET schema ...",
		},
	}
	s.Hub.Broadcast(scope, id, map[string]any{"type": "done", "data": map[string]any{"id": id, "scope": scope, "payload": payload}})
}

func (s *PipelineService) simulatePipeline(id string) {
	scope := "pipeline"
	s.Hub.Broadcast(scope, id, map[string]any{"type": "queued", "data": map[string]any{"id": id, "scope": scope}})
	time.Sleep(300 * time.Millisecond)
	s.Hub.Broadcast(scope, id, map[string]any{"type": "started", "data": map[string]any{"id": id, "scope": scope}})
	stages := []string{"extract", "transform", "load", "validate"}
	for i, st := range stages {
		p := (i + 1) * 25
		msg := fmt.Sprintf("stage %s", st)
		s.Hub.Broadcast(scope, id, map[string]any{"type": "progress", "data": map[string]any{"id": id, "scope": scope, "percent": p, "stage": st, "message": msg}})
		s.Hub.Broadcast(scope, id, map[string]any{"type": "log", "data": map[string]any{"id": id, "scope": scope, "level": "info", "line": msg}})
		time.Sleep(250 * time.Millisecond)
	}
	payload := map[string]any{
		"status":  "success",
		"message": "Pipeline completed",
		"stats": map[string]any{
			"rows_processed": 12345,
			"duration_ms":    1000,
		},
	}
	s.Hub.Broadcast(scope, id, map[string]any{"type": "done", "data": map[string]any{"id": id, "scope": scope, "payload": payload}})
}
