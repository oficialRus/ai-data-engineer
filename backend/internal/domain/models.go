package domain

import "encoding/json"

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
