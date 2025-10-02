package domain

import (
	"encoding/json"
	"time"
)

type AnalyzePreviewColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type AnalyzePreview struct {
	Columns  []AnalyzePreviewColumn `json:"columns"`
	Rows     []map[string]any       `json:"rows"`
	RowCount int                    `json:"rowCount"`
}

type AnalyzeRequest struct {
	Preview AnalyzePreview `json:"preview"`
}

type AnalyzeResponse struct {
	JobID string `json:"job_id"`
}

type PipelineSourceFile struct {
	Kind      string `json:"kind"`
	Type      string `json:"type"`
	PathOrURL string `json:"pathOrUrl"`
}

type PipelineSourcePG struct {
	Kind            string  `json:"kind"`
	DSN             *string `json:"dsn,omitempty"`
	Host            *string `json:"host,omitempty"`
	Port            *int    `json:"port,omitempty"`
	Database        *string `json:"database,omitempty"`
	Table           *string `json:"table,omitempty"`
	UpdatedAtColumn *string `json:"updatedAtColumn,omitempty"`
}

type PipelinePreview struct {
	Columns  []AnalyzePreviewColumn `json:"columns"`
	Rows     []map[string]any       `json:"rows"`
	RowCount int                    `json:"rowCount"`
}

type PipelineDDL struct {
	ClickHouse string `json:"clickhouse"`
	PostgreSQL string `json:"postgresql"`
	HDFS       string `json:"hdfs"`
}

type PipelineSchedule struct {
	Cron              string `json:"cron"`
	IncrementalMode   string `json:"incrementalMode"`
	IncrementalColumn string `json:"incrementalColumn,omitempty"`
}

type CreatePipelineRequest struct {
	SourceType string           `json:"sourceType"`
	Source     json.RawMessage  `json:"source"`
	Preview    *PipelinePreview `json:"preview,omitempty"`
	Target     *string          `json:"target,omitempty"`
	DDL        PipelineDDL      `json:"ddl"`
	Schedule   PipelineSchedule `json:"schedule"`
}

type CreatePipelineResponse struct {
	ID string `json:"id"`
}

// Pipeline представляет сохраненный пайплайн
type Pipeline struct {
	ID          string                 `json:"id" db:"id"`
	Name        string                 `json:"name" db:"name"`
	SourceType  string                 `json:"source_type" db:"source_type"`
	Source      json.RawMessage        `json:"source" db:"source"`
	Target      *string                `json:"target" db:"target"`
	DDL         json.RawMessage        `json:"ddl" db:"ddl"`
	Schedule    json.RawMessage        `json:"schedule" db:"schedule"`
	Status      string                 `json:"status" db:"status"`
	CreatedAt   time.Time              `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at" db:"updated_at"`
	DAGFilePath *string                `json:"dag_file_path,omitempty" db:"dag_file_path"`
	LastRun     *time.Time             `json:"last_run,omitempty" db:"last_run"`
	RunCount    int                    `json:"run_count" db:"run_count"`
	Metadata    map[string]interface{} `json:"metadata,omitempty"`
}

// PipelineListItem представляет элемент списка пайплайнов
type PipelineListItem struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	SourceType string     `json:"source_type"`
	Target     *string    `json:"target"`
	Status     string     `json:"status"`
	CreatedAt  time.Time  `json:"created_at"`
	LastRun    *time.Time `json:"last_run,omitempty"`
	RunCount   int        `json:"run_count"`
}
