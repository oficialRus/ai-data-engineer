package usecase

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/user/ai-data-engineer/backend/internal/config"
	domain "github.com/user/ai-data-engineer/backend/internal/domain"
)

type DAGGeneratorService struct {
	Cfg config.AppConfig
}

type DAGTemplateData struct {
	// Основная информация
	PipelineName string
	PipelineID   string
	DagID        string
	CreatedAt    string

	// Конфигурация источника
	SourceType       string
	SourceConfig     string
	SourceConfigJSON string

	// Конфигурация цели
	Target           string
	ConnectionID     string
	TargetConfigJSON string

	// DDL и расписание
	DDLStatementsJSON string
	ScheduleInterval  string
	CreateDDL         bool

	// Дополнительные параметры
	SchemaVersion     int
	IncrementalMode   string
	IncrementalColumn string
}

func (s *DAGGeneratorService) GenerateDAG(pipelineReq domain.CreatePipelineRequest, pipelineID string) error {
	log.Printf("[DAG-GEN] Starting DAG generation for pipeline %s", pipelineID)
	log.Printf("[DAG-GEN] Source type: %s, Target: %s", pipelineReq.SourceType, s.getTargetOrDefault(pipelineReq.Target))
	log.Printf("[DAG-GEN] Schedule: %s, Incremental mode: %s", pipelineReq.Schedule.Cron, pipelineReq.Schedule.IncrementalMode)

	// Подготовка данных для шаблона
	log.Printf("[DAG-GEN] Preparing template data...")
	templateData, err := s.prepareTemplateData(pipelineReq, pipelineID)
	if err != nil {
		log.Printf("[DAG-GEN] ERROR: Failed to prepare template data: %v", err)
		return fmt.Errorf("failed to prepare template data: %w", err)
	}
	log.Printf("[DAG-GEN] Template data prepared successfully. DAG ID: %s", templateData.DagID)

	// Загрузка и рендеринг шаблона
	log.Printf("[DAG-GEN] Rendering DAG template...")
	dagContent, err := s.renderTemplate(templateData)
	if err != nil {
		log.Printf("[DAG-GEN] ERROR: Failed to render DAG template: %v", err)
		return fmt.Errorf("failed to render DAG template: %w", err)
	}
	log.Printf("[DAG-GEN] DAG template rendered successfully. Content length: %d bytes", len(dagContent))

	// Сохранение DAG файла
	log.Printf("[DAG-GEN] Saving DAG file...")
	if err := s.saveDAGFile(templateData.DagID, dagContent); err != nil {
		log.Printf("[DAG-GEN] ERROR: Failed to save DAG file: %v", err)
		return fmt.Errorf("failed to save DAG file: %w", err)
	}

	log.Printf("[DAG-GEN] DAG generation completed successfully for pipeline %s", pipelineID)
	return nil
}

func (s *DAGGeneratorService) prepareTemplateData(req domain.CreatePipelineRequest, pipelineID string) (*DAGTemplateData, error) {
	log.Printf("[DAG-GEN] Generating DAG ID for pipeline %s", pipelineID)

	// Генерация DAG ID
	schemaVersion := 1 // В будущем можно сделать динамическим
	dagID := fmt.Sprintf("pipe_%s_v%d", strings.ReplaceAll(pipelineID, "-", "_"), schemaVersion)
	log.Printf("[DAG-GEN] Generated DAG ID: %s", dagID)

	// Подготовка конфигурации источника
	log.Printf("[DAG-GEN] Marshaling source config...")
	sourceConfigJSON, err := json.Marshal(req.Source)
	if err != nil {
		log.Printf("[DAG-GEN] ERROR: Failed to marshal source config: %v", err)
		return nil, fmt.Errorf("failed to marshal source config: %w", err)
	}
	log.Printf("[DAG-GEN] Source config marshaled successfully. Length: %d bytes", len(sourceConfigJSON))

	// Определение connection ID для целевой системы
	connectionID := s.getConnectionID(req.Target)
	log.Printf("[DAG-GEN] Determined connection ID: %s for target: %s", connectionID, s.getTargetOrDefault(req.Target))

	// Подготовка конфигурации цели
	log.Printf("[DAG-GEN] Preparing target config...")
	targetConfig := map[string]interface{}{
		"type":       req.Target,
		"connection": connectionID,
	}
	targetConfigJSON, err := json.Marshal(targetConfig)
	if err != nil {
		log.Printf("[DAG-GEN] ERROR: Failed to marshal target config: %v", err)
		return nil, fmt.Errorf("failed to marshal target config: %w", err)
	}
	log.Printf("[DAG-GEN] Target config prepared successfully")

	// Подготовка DDL statements
	log.Printf("[DAG-GEN] Marshaling DDL statements...")
	ddlStatementsJSON, err := json.Marshal(req.DDL)
	if err != nil {
		log.Printf("[DAG-GEN] ERROR: Failed to marshal DDL statements: %v", err)
		return nil, fmt.Errorf("failed to marshal DDL statements: %w", err)
	}
	log.Printf("[DAG-GEN] DDL statements marshaled successfully. Length: %d bytes", len(ddlStatementsJSON))

	// Определение нужности создания DDL
	createDDL := req.Target != nil && *req.Target != ""
	log.Printf("[DAG-GEN] Create DDL flag: %t", createDDL)

	pipelineName := fmt.Sprintf("Pipeline_%s", pipelineID[:8])
	log.Printf("[DAG-GEN] Pipeline name: %s", pipelineName)

	templateData := &DAGTemplateData{
		PipelineName: pipelineName,
		PipelineID:   pipelineID,
		DagID:        dagID,
		CreatedAt:    time.Now().Format("2006-01-02 15:04:05"),

		SourceType:       req.SourceType,
		SourceConfig:     string(req.Source),
		SourceConfigJSON: string(sourceConfigJSON),

		Target:           s.getTargetOrDefault(req.Target),
		ConnectionID:     connectionID,
		TargetConfigJSON: string(targetConfigJSON),

		DDLStatementsJSON: string(ddlStatementsJSON),
		ScheduleInterval:  req.Schedule.Cron,
		CreateDDL:         createDDL,

		SchemaVersion:     schemaVersion,
		IncrementalMode:   req.Schedule.IncrementalMode,
		IncrementalColumn: req.Schedule.IncrementalColumn,
	}

	log.Printf("[DAG-GEN] Template data structure created successfully")
	return templateData, nil
}

func (s *DAGGeneratorService) getConnectionID(target *string) string {
	if target == nil {
		return "pg_target" // по умолчанию
	}

	switch *target {
	case "postgresql":
		return "pg_target"
	case "clickhouse":
		return "ch_target"
	case "hdfs":
		return "hdfs_target"
	default:
		return "pg_target"
	}
}

func (s *DAGGeneratorService) getTargetOrDefault(target *string) string {
	if target == nil {
		return "postgresql"
	}
	return *target
}

func (s *DAGGeneratorService) renderTemplate(data *DAGTemplateData) (string, error) {
	// Путь к шаблону
	templatePath := "backend/templates/dag_template.py"
	log.Printf("[DAG-GEN] Reading template file: %s", templatePath)

	// Чтение шаблона
	templateContent, err := os.ReadFile(templatePath)
	if err != nil {
		log.Printf("[DAG-GEN] ERROR: Failed to read template file %s: %v", templatePath, err)
		return "", fmt.Errorf("failed to read template file: %w", err)
	}
	log.Printf("[DAG-GEN] Template file read successfully. Size: %d bytes", len(templateContent))

	// Создание и парсинг шаблона
	log.Printf("[DAG-GEN] Parsing template...")
	tmpl, err := template.New("dag").Parse(string(templateContent))
	if err != nil {
		log.Printf("[DAG-GEN] ERROR: Failed to parse template: %v", err)
		return "", fmt.Errorf("failed to parse template: %w", err)
	}
	log.Printf("[DAG-GEN] Template parsed successfully")

	// Рендеринг шаблона
	log.Printf("[DAG-GEN] Executing template with data...")
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		log.Printf("[DAG-GEN] ERROR: Failed to execute template: %v", err)
		return "", fmt.Errorf("failed to execute template: %w", err)
	}
	log.Printf("[DAG-GEN] Template executed successfully. Generated content size: %d bytes", buf.Len())

	return buf.String(), nil
}

func (s *DAGGeneratorService) saveDAGFile(dagID, content string) error {
	// Путь к папке DAG файлов Airflow
	dagsDir := "ops/airflow/dags"
	log.Printf("[DAG-GEN] Target directory: %s", dagsDir)

	// Создание папки если не существует
	log.Printf("[DAG-GEN] Ensuring directory exists...")
	if err := os.MkdirAll(dagsDir, 0755); err != nil {
		log.Printf("[DAG-GEN] ERROR: Failed to create dags directory %s: %v", dagsDir, err)
		return fmt.Errorf("failed to create dags directory: %w", err)
	}
	log.Printf("[DAG-GEN] Directory ensured successfully")

	// Имя файла
	filename := fmt.Sprintf("%s.py", dagID)
	filepath := filepath.Join(dagsDir, filename)
	log.Printf("[DAG-GEN] Target file path: %s", filepath)

	// Запись файла
	log.Printf("[DAG-GEN] Writing DAG file...")
	if err := os.WriteFile(filepath, []byte(content), 0644); err != nil {
		log.Printf("[DAG-GEN] ERROR: Failed to write DAG file %s: %v", filepath, err)
		return fmt.Errorf("failed to write DAG file: %w", err)
	}
	log.Printf("[DAG-GEN] DAG file written successfully: %s (%d bytes)", filepath, len(content))

	return nil
}

func (s *DAGGeneratorService) DeleteDAG(pipelineID string) error {
	log.Printf("[DAG-GEN] Starting DAG deletion for pipeline %s", pipelineID)

	// Генерация имени файла (аналогично генерации DAG ID)
	schemaVersion := 1
	dagID := fmt.Sprintf("pipe_%s_v%d", strings.ReplaceAll(pipelineID, "-", "_"), schemaVersion)
	log.Printf("[DAG-GEN] Generated DAG ID for deletion: %s", dagID)

	// Путь к файлу
	dagsDir := "ops/airflow/dags"
	filename := fmt.Sprintf("%s.py", dagID)
	filepath := filepath.Join(dagsDir, filename)
	log.Printf("[DAG-GEN] Target file for deletion: %s", filepath)

	// Удаление файла
	log.Printf("[DAG-GEN] Attempting to delete DAG file...")
	if err := os.Remove(filepath); err != nil {
		// Если файл не существует, это не ошибка
		if os.IsNotExist(err) {
			log.Printf("[DAG-GEN] DAG file does not exist (already deleted): %s", filepath)
			return nil
		}
		log.Printf("[DAG-GEN] ERROR: Failed to delete DAG file %s: %v", filepath, err)
		return fmt.Errorf("failed to delete DAG file: %w", err)
	}

	log.Printf("[DAG-GEN] DAG file deleted successfully: %s", filepath)
	return nil
}

func (s *DAGGeneratorService) ListDAGs() ([]string, error) {
	dagsDir := "ops/airflow/dags"
	log.Printf("[DAG-GEN] Listing DAG files in directory: %s", dagsDir)

	entries, err := os.ReadDir(dagsDir)
	if err != nil {
		log.Printf("[DAG-GEN] ERROR: Failed to read dags directory %s: %v", dagsDir, err)
		return nil, fmt.Errorf("failed to read dags directory: %w", err)
	}
	log.Printf("[DAG-GEN] Found %d entries in directory", len(entries))

	var dags []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".py") && strings.HasPrefix(entry.Name(), "pipe_") {
			// Убираем расширение .py
			dagName := strings.TrimSuffix(entry.Name(), ".py")
			dags = append(dags, dagName)
			log.Printf("[DAG-GEN] Found generated DAG: %s", dagName)
		}
	}

	log.Printf("[DAG-GEN] Total generated DAGs found: %d", len(dags))
	return dags, nil
}
