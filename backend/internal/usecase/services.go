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
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/user/ai-data-engineer/backend/internal/config"
	domain "github.com/user/ai-data-engineer/backend/internal/domain"
	infra "github.com/user/ai-data-engineer/backend/internal/infra/ws"
)

type AnalyzeService struct {
	Hub *infra.Hub
	Cfg config.AppConfig
}

type PipelineService struct {
	Hub          *infra.Hub
	Cfg          config.AppConfig
	DAGGenerator *DAGGeneratorService
	AirflowSvc   *AirflowService
}

func (s *AnalyzeService) StartAnalyze(preview json.RawMessage) (string, error) {
	id := uuid.NewString()
	go s.runAnalyze(id, preview)
	return id, nil
}

func (s *PipelineService) CreatePipeline(req json.RawMessage) (string, error) {
	id := uuid.NewString()

	// Парсим запрос для генерации DAG
	var pipelineReq domain.CreatePipelineRequest
	if err := json.Unmarshal(req, &pipelineReq); err != nil {
		return "", fmt.Errorf("failed to parse pipeline request: %w", err)
	}

	// Генерируем DAG файл
	if s.DAGGenerator != nil {
		if err := s.DAGGenerator.GenerateDAG(pipelineReq, id); err != nil {
			log.Printf("[PIPELINE] Failed to generate DAG for pipeline %s: %v", id, err)
			// Не прерываем создание пайплайна из-за ошибки генерации DAG
		} else {
			log.Printf("[PIPELINE] DAG generated successfully for pipeline %s", id)
		}
	}

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

	// Преобразуем ML ответ в формат, ожидаемый клиентом
	clientPayload := transformMLResponseToClientFormat(mlResp)
	log.Printf("[ANALYZE] Transformed payload for client: %+v", clientPayload)

	// Отправляем результат через WebSocket
	doneMessage := map[string]any{"type": "done", "data": map[string]any{"id": id, "scope": scope, "payload": clientPayload}}
	log.Printf("[ANALYZE] Broadcasting 'done' message to WebSocket clients for job %s", id)
	log.Printf("[ANALYZE] Done message payload size: %d bytes", len(fmt.Sprintf("%+v", clientPayload)))
	s.Hub.Broadcast(scope, id, doneMessage)
	log.Printf("[ANALYZE] Successfully broadcasted 'done' message for job %s", id)
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

// transformMLResponseToClientFormat преобразует ответ ML в формат, ожидаемый клиентом
func transformMLResponseToClientFormat(mlResp map[string]any) map[string]any {
	log.Printf("[TRANSFORM] Converting ML response to client format")

	// Извлекаем данные из ML ответа
	var hasNumeric, hasTemporal, hasText bool
	var recordCount int

	if dataProfile, ok := mlResp["data_profile"].(map[string]any); ok {
		hasNumeric, _ = dataProfile["has_numeric"].(bool)
		hasTemporal, _ = dataProfile["has_temporal"].(bool)
		hasText, _ = dataProfile["has_text"].(bool)
		recordCount, _ = dataProfile["record_count"].(int)
	}

	// Определяем рекомендуемую целевую систему на основе анализа данных
	var target string
	var confidence float64
	var rationale string
	var scheduleHint string

	if hasNumeric && hasTemporal && recordCount > 1000 {
		target = "clickhouse"
		confidence = 0.85
		rationale = "ClickHouse рекомендуется для аналитических запросов с временными рядами и числовыми данными"
		scheduleHint = "0 */6 * * *" // каждые 6 часов
	} else if hasText && recordCount < 10000 {
		target = "postgresql"
		confidence = 0.75
		rationale = "PostgreSQL подходит для транзакционных данных с текстовыми полями"
		scheduleHint = "0 2 * * *" // ежедневно в 2:00
	} else {
		target = "hdfs"
		confidence = 0.60
		rationale = "HDFS рекомендуется для больших объемов данных и долгосрочного хранения"
		scheduleHint = "0 1 * * 0" // еженедельно
	}

	log.Printf("[TRANSFORM] Recommended target: %s (confidence: %.2f)", target, confidence)

	// Генерируем DDL для каждой целевой системы
	ddl := generateDDLForTargets(mlResp)

	// Формируем ответ в ожидаемом клиентом формате
	clientPayload := map[string]any{
		"recommendation": map[string]any{
			"target":        target,
			"confidence":    confidence,
			"rationale":     rationale,
			"schedule_hint": scheduleHint,
		},
		"ddl": ddl,
	}

	log.Printf("[TRANSFORM] Client payload created successfully")
	return clientPayload
}

// generateDDLForTargets генерирует DDL для всех целевых систем
func generateDDLForTargets(mlResp map[string]any) map[string]string {
	log.Printf("[DDL] Generating DDL for all target systems")

	var columns []string
	var dtypes map[string]any

	// Извлекаем информацию о колонках
	if features, ok := mlResp["features"].(map[string]any); ok {
		if cols, ok := features["columns"].([]any); ok {
			for _, col := range cols {
				if colName, ok := col.(string); ok {
					columns = append(columns, colName)
				}
			}
		}
		dtypes, _ = features["dtypes"].(map[string]any)
	}

	tableName := "imported_data"

	// ClickHouse DDL
	clickhouseDDL := generateClickHouseDDL(tableName, columns, dtypes)

	// PostgreSQL DDL
	postgresqlDDL := generatePostgreSQLDDL(tableName, columns, dtypes)

	// HDFS DDL (Hive-style)
	hdfsDDL := generateHDFSDDL(tableName, columns, dtypes)

	return map[string]string{
		"clickhouse": clickhouseDDL,
		"postgresql": postgresqlDDL,
		"hdfs":       hdfsDDL,
	}
}

func generateClickHouseDDL(tableName string, columns []string, dtypes map[string]any) string {
	log.Printf("[DDL] [CLICKHOUSE] Generating DDL for table: %s", tableName)
	ddl := fmt.Sprintf("CREATE TABLE %s (\n", tableName)

	var primaryKey string

	for i, col := range columns {
		chType := "String" // default
		if dtypes != nil {
			if dtype, ok := dtypes[col].(string); ok {
				log.Printf("[DDL] [CLICKHOUSE] Column %s has type: %s", col, dtype)
				switch dtype {
				case "int64":
					chType = "Int64"
					if strings.Contains(strings.ToLower(col), "id") && primaryKey == "" {
						primaryKey = col
					}
				case "uint32":
					chType = "UInt32"
					if strings.Contains(strings.ToLower(col), "id") && primaryKey == "" {
						primaryKey = col
					}
				case "uint16":
					chType = "UInt16"
				case "uint8":
					chType = "UInt8"
				case "float32":
					chType = "Float32"
				case "float64":
					chType = "Float64"
				case "bool":
					chType = "Bool"
				case "category":
					chType = "LowCardinality(String)"
				case "object":
					if strings.Contains(strings.ToLower(col), "date") || strings.Contains(strings.ToLower(col), "time") || strings.Contains(strings.ToLower(col), "created") || strings.Contains(strings.ToLower(col), "updated") {
						chType = "DateTime"
					} else {
						chType = "String"
					}
				default:
					chType = "String"
				}
			}
		}

		ddl += fmt.Sprintf("    `%s` %s", col, chType)
		if i < len(columns)-1 {
			ddl += ","
		}
		ddl += "\n"
	}

	// Определяем ORDER BY
	orderBy := "tuple()"
	if primaryKey != "" {
		orderBy = fmt.Sprintf("`%s`", primaryKey)
		log.Printf("[DDL] [CLICKHOUSE] Using primary key for ORDER BY: %s", primaryKey)
	} else if len(columns) > 0 {
		orderBy = fmt.Sprintf("`%s`", columns[0])
		log.Printf("[DDL] [CLICKHOUSE] Using first column for ORDER BY: %s", columns[0])
	}

	ddl += fmt.Sprintf(") ENGINE = MergeTree()\nORDER BY %s;", orderBy)
	log.Printf("[DDL] [CLICKHOUSE] Generated DDL successfully")
	return ddl
}

func generatePostgreSQLDDL(tableName string, columns []string, dtypes map[string]any) string {
	log.Printf("[DDL] [POSTGRESQL] Generating DDL for table: %s", tableName)
	ddl := fmt.Sprintf("CREATE TABLE %s (\n", tableName)

	var primaryKey string

	for i, col := range columns {
		pgType := "TEXT" // default
		constraints := ""

		if dtypes != nil {
			if dtype, ok := dtypes[col].(string); ok {
				log.Printf("[DDL] [POSTGRESQL] Column %s has type: %s", col, dtype)
				switch dtype {
				case "int64":
					pgType = "BIGINT"
					if strings.Contains(strings.ToLower(col), "id") && primaryKey == "" {
						primaryKey = col
						constraints = " PRIMARY KEY"
					}
				case "uint32":
					pgType = "INTEGER"
					if strings.Contains(strings.ToLower(col), "id") && primaryKey == "" {
						primaryKey = col
						constraints = " PRIMARY KEY"
					}
				case "uint16":
					pgType = "INTEGER"
				case "uint8":
					pgType = "SMALLINT"
				case "float32":
					pgType = "REAL"
				case "float64":
					pgType = "DOUBLE PRECISION"
				case "bool":
					pgType = "BOOLEAN"
				case "category":
					pgType = "VARCHAR(255)"
				case "object":
					if strings.Contains(strings.ToLower(col), "date") || strings.Contains(strings.ToLower(col), "time") || strings.Contains(strings.ToLower(col), "created") || strings.Contains(strings.ToLower(col), "updated") {
						pgType = "TIMESTAMP WITH TIME ZONE"
					} else if strings.Contains(strings.ToLower(col), "email") {
						pgType = "VARCHAR(255)"
					} else {
						pgType = "TEXT"
					}
				default:
					pgType = "TEXT"
				}
			}
		}

		ddl += fmt.Sprintf("    \"%s\" %s%s", col, pgType, constraints)
		if i < len(columns)-1 {
			ddl += ","
		}
		ddl += "\n"
	}

	ddl += ");"

	// Добавляем индексы для часто используемых колонок
	for _, col := range columns {
		colLower := strings.ToLower(col)
		if strings.Contains(colLower, "date") || strings.Contains(colLower, "time") || strings.Contains(colLower, "created") || strings.Contains(colLower, "updated") {
			ddl += fmt.Sprintf("\nCREATE INDEX idx_%s_%s ON %s (\"%s\");", tableName, col, tableName, col)
		}
	}

	log.Printf("[DDL] [POSTGRESQL] Generated DDL successfully")
	return ddl
}

func generateHDFSDDL(tableName string, columns []string, dtypes map[string]any) string {
	log.Printf("[DDL] [HDFS] Generating Hive DDL for table: %s", tableName)
	ddl := fmt.Sprintf("CREATE TABLE %s (\n", tableName)

	for i, col := range columns {
		hiveType := "STRING" // default
		if dtypes != nil {
			if dtype, ok := dtypes[col].(string); ok {
				log.Printf("[DDL] [HDFS] Column %s has type: %s", col, dtype)
				switch dtype {
				case "int64":
					hiveType = "BIGINT"
				case "uint32":
					hiveType = "INT"
				case "uint16":
					hiveType = "SMALLINT"
				case "uint8":
					hiveType = "TINYINT"
				case "float32":
					hiveType = "FLOAT"
				case "float64":
					hiveType = "DOUBLE"
				case "bool":
					hiveType = "BOOLEAN"
				case "category":
					hiveType = "STRING"
				case "object":
					if strings.Contains(strings.ToLower(col), "date") || strings.Contains(strings.ToLower(col), "time") || strings.Contains(strings.ToLower(col), "created") || strings.Contains(strings.ToLower(col), "updated") {
						hiveType = "TIMESTAMP"
					} else {
						hiveType = "STRING"
					}
				default:
					hiveType = "STRING"
				}
			}
		}

		ddl += fmt.Sprintf("    `%s` %s", col, hiveType)
		if i < len(columns)-1 {
			ddl += ","
		}
		ddl += "\n"
	}

	ddl += ")\nSTORED AS PARQUET\nLOCATION '/data/warehouse/" + tableName + "'\nTBLPROPERTIES (\n"
	ddl += "    'parquet.compression'='SNAPPY',\n"
	ddl += "    'transactional'='true'\n"
	ddl += ");"

	log.Printf("[DDL] [HDFS] Generated DDL successfully")
	return ddl
}
