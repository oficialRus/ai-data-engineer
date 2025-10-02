package usecase

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/user/ai-data-engineer/backend/internal/config"
	"github.com/user/ai-data-engineer/backend/internal/domain"
)

type PipelineStorageService struct {
	Cfg config.AppConfig
}

func (s *PipelineStorageService) InitializeTables() error {
	log.Printf("[PIPELINE_STORAGE] Initializing pipeline tables")

	db, err := sql.Open("postgres", s.Cfg.Database.PostgreSQLDataDSN)
	if err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to connect to database: %v", err)
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Создаем таблицу для хранения пайплайнов
	createTableQuery := `
		CREATE TABLE IF NOT EXISTS pipelines (
			id VARCHAR(255) PRIMARY KEY,
			name VARCHAR(500) NOT NULL,
			source_type VARCHAR(50) NOT NULL,
			source JSONB NOT NULL,
			target VARCHAR(50),
			ddl JSONB NOT NULL,
			schedule JSONB NOT NULL,
			status VARCHAR(50) DEFAULT 'created',
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			dag_file_path TEXT,
			last_run TIMESTAMP WITH TIME ZONE,
			run_count INTEGER DEFAULT 0
		);

		-- Создаем индексы для быстрого поиска
		CREATE INDEX IF NOT EXISTS idx_pipelines_status ON pipelines(status);
		CREATE INDEX IF NOT EXISTS idx_pipelines_source_type ON pipelines(source_type);
		CREATE INDEX IF NOT EXISTS idx_pipelines_created_at ON pipelines(created_at);
		CREATE INDEX IF NOT EXISTS idx_pipelines_target ON pipelines(target);
	`

	if _, err := db.ExecContext(ctx, createTableQuery); err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to create tables: %v", err)
		return fmt.Errorf("failed to create tables: %w", err)
	}

	log.Printf("[PIPELINE_STORAGE] Tables initialized successfully")
	return nil
}

func (s *PipelineStorageService) SavePipeline(pipeline domain.Pipeline) error {
	log.Printf("[PIPELINE_STORAGE] Saving pipeline: %s", pipeline.ID)

	db, err := sql.Open("postgres", s.Cfg.Database.PostgreSQLDataDSN)
	if err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to connect to database: %v", err)
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
		INSERT INTO pipelines (
			id, name, source_type, source, target, ddl, schedule, 
			status, created_at, updated_at, dag_file_path
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
		) ON CONFLICT (id) DO UPDATE SET
			name = EXCLUDED.name,
			source_type = EXCLUDED.source_type,
			source = EXCLUDED.source,
			target = EXCLUDED.target,
			ddl = EXCLUDED.ddl,
			schedule = EXCLUDED.schedule,
			status = EXCLUDED.status,
			updated_at = EXCLUDED.updated_at,
			dag_file_path = EXCLUDED.dag_file_path
	`

	_, err = db.ExecContext(ctx, query,
		pipeline.ID,
		pipeline.Name,
		pipeline.SourceType,
		pipeline.Source,
		pipeline.Target,
		pipeline.DDL,
		pipeline.Schedule,
		pipeline.Status,
		pipeline.CreatedAt,
		pipeline.UpdatedAt,
		pipeline.DAGFilePath,
	)

	if err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to save pipeline: %v", err)
		return fmt.Errorf("failed to save pipeline: %w", err)
	}

	log.Printf("[PIPELINE_STORAGE] Pipeline saved successfully: %s", pipeline.ID)
	return nil
}

func (s *PipelineStorageService) GetPipelines() ([]domain.PipelineListItem, error) {
	log.Printf("[PIPELINE_STORAGE] Getting all pipelines")

	db, err := sql.Open("postgres", s.Cfg.Database.PostgreSQLDataDSN)
	if err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to connect to database: %v", err)
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
		SELECT id, name, source_type, target, status, created_at, last_run, run_count
		FROM pipelines 
		ORDER BY created_at DESC
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to query pipelines: %v", err)
		return nil, fmt.Errorf("failed to query pipelines: %w", err)
	}
	defer rows.Close()

	var pipelines []domain.PipelineListItem
	for rows.Next() {
		var pipeline domain.PipelineListItem
		err := rows.Scan(
			&pipeline.ID,
			&pipeline.Name,
			&pipeline.SourceType,
			&pipeline.Target,
			&pipeline.Status,
			&pipeline.CreatedAt,
			&pipeline.LastRun,
			&pipeline.RunCount,
		)
		if err != nil {
			log.Printf("[PIPELINE_STORAGE] ERROR: Failed to scan pipeline row: %v", err)
			continue
		}
		pipelines = append(pipelines, pipeline)
	}

	if err := rows.Err(); err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Error iterating pipeline rows: %v", err)
		return nil, fmt.Errorf("error reading pipelines: %w", err)
	}

	log.Printf("[PIPELINE_STORAGE] Retrieved %d pipelines", len(pipelines))
	return pipelines, nil
}

func (s *PipelineStorageService) GetPipeline(id string) (*domain.Pipeline, error) {
	log.Printf("[PIPELINE_STORAGE] Getting pipeline: %s", id)

	db, err := sql.Open("postgres", s.Cfg.Database.PostgreSQLDataDSN)
	if err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to connect to database: %v", err)
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
		SELECT id, name, source_type, source, target, ddl, schedule, 
			   status, created_at, updated_at, dag_file_path, last_run, run_count
		FROM pipelines 
		WHERE id = $1
	`

	var pipeline domain.Pipeline
	err = db.QueryRowContext(ctx, query, id).Scan(
		&pipeline.ID,
		&pipeline.Name,
		&pipeline.SourceType,
		&pipeline.Source,
		&pipeline.Target,
		&pipeline.DDL,
		&pipeline.Schedule,
		&pipeline.Status,
		&pipeline.CreatedAt,
		&pipeline.UpdatedAt,
		&pipeline.DAGFilePath,
		&pipeline.LastRun,
		&pipeline.RunCount,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("[PIPELINE_STORAGE] Pipeline not found: %s", id)
			return nil, fmt.Errorf("pipeline not found: %s", id)
		}
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to get pipeline: %v", err)
		return nil, fmt.Errorf("failed to get pipeline: %w", err)
	}

	log.Printf("[PIPELINE_STORAGE] Retrieved pipeline: %s", id)
	return &pipeline, nil
}

func (s *PipelineStorageService) UpdatePipelineStatus(id string, status string) error {
	log.Printf("[PIPELINE_STORAGE] Updating pipeline status: %s -> %s", id, status)

	db, err := sql.Open("postgres", s.Cfg.Database.PostgreSQLDataDSN)
	if err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to connect to database: %v", err)
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
		UPDATE pipelines 
		SET status = $1, updated_at = NOW()
		WHERE id = $2
	`

	result, err := db.ExecContext(ctx, query, status, id)
	if err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to update pipeline status: %v", err)
		return fmt.Errorf("failed to update pipeline status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to get rows affected: %v", err)
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		log.Printf("[PIPELINE_STORAGE] Pipeline not found for status update: %s", id)
		return fmt.Errorf("pipeline not found: %s", id)
	}

	log.Printf("[PIPELINE_STORAGE] Pipeline status updated successfully: %s", id)
	return nil
}

func (s *PipelineStorageService) UpdatePipelineRun(id string) error {
	log.Printf("[PIPELINE_STORAGE] Updating pipeline run info: %s", id)

	db, err := sql.Open("postgres", s.Cfg.Database.PostgreSQLDataDSN)
	if err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to connect to database: %v", err)
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
		UPDATE pipelines 
		SET last_run = NOW(), run_count = run_count + 1, updated_at = NOW()
		WHERE id = $1
	`

	result, err := db.ExecContext(ctx, query, id)
	if err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to update pipeline run: %v", err)
		return fmt.Errorf("failed to update pipeline run: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to get rows affected: %v", err)
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		log.Printf("[PIPELINE_STORAGE] Pipeline not found for run update: %s", id)
		return fmt.Errorf("pipeline not found: %s", id)
	}

	log.Printf("[PIPELINE_STORAGE] Pipeline run info updated successfully: %s", id)
	return nil
}

func (s *PipelineStorageService) DeletePipeline(id string) error {
	log.Printf("[PIPELINE_STORAGE] Deleting pipeline: %s", id)

	db, err := sql.Open("postgres", s.Cfg.Database.PostgreSQLDataDSN)
	if err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to connect to database: %v", err)
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `DELETE FROM pipelines WHERE id = $1`

	result, err := db.ExecContext(ctx, query, id)
	if err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to delete pipeline: %v", err)
		return fmt.Errorf("failed to delete pipeline: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Printf("[PIPELINE_STORAGE] ERROR: Failed to get rows affected: %v", err)
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		log.Printf("[PIPELINE_STORAGE] Pipeline not found for deletion: %s", id)
		return fmt.Errorf("pipeline not found: %s", id)
	}

	log.Printf("[PIPELINE_STORAGE] Pipeline deleted successfully: %s", id)
	return nil
}
