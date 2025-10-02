package httpiface

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/user/ai-data-engineer/backend/internal/config"
	domain "github.com/user/ai-data-engineer/backend/internal/domain"
	"github.com/user/ai-data-engineer/backend/internal/usecase"
)

const maxRequestBodyBytes = 1 << 20 // 1 MiB

type HTTPHandlers struct {
	AnalyzeSvc  *usecase.AnalyzeService
	PipelineSvc *usecase.PipelineService
	PreviewSvc  *usecase.PreviewService
	AirflowSvc  *usecase.AirflowService
	DatabaseSvc *usecase.DatabaseService
	Cfg         config.AppConfig
}

func (h *HTTPHandlers) Preview(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := fmt.Sprintf("preview-%d", start.UnixNano())
	log.Printf("[%s] [PREVIEW] Starting request from %s", requestID, r.RemoteAddr)
	log.Printf("[%s] [PREVIEW] Content-Type: %s", requestID, r.Header.Get("Content-Type"))
	log.Printf("[%s] [PREVIEW] Content-Length: %s", requestID, r.Header.Get("Content-Length"))
	log.Printf("[%s] [PREVIEW] Request URL: %s", requestID, r.URL.String())
	log.Printf("[%s] [PREVIEW] Request Method: %s", requestID, r.Method)

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[%s] [PREVIEW] PANIC: %v", requestID, r)
			writeErrorWithRequestID(w, http.StatusInternalServerError, "Internal server error", requestID)
		}
		duration := time.Since(start)
		log.Printf("[%s] [PREVIEW] Request completed in %v", requestID, duration)
	}()

	// Support multipart (file) and JSON (pg)
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		log.Printf("[%s] [PREVIEW] Processing multipart/form-data request", requestID)
		// file upload
		log.Printf("[%s] [PREVIEW] Parsing multipart form with max size: %d bytes", requestID, h.Cfg.Limits.MaxFileSizeBytes)
		log.Printf("[%s] [PREVIEW] Starting ParseMultipartForm...", requestID)

		parseStart := time.Now()
		if err := r.ParseMultipartForm(h.Cfg.Limits.MaxFileSizeBytes); err != nil {
			parseDuration := time.Since(parseStart)
			log.Printf("[%s] [PREVIEW] ERROR: Failed to parse multipart form after %v: %v", requestID, parseDuration, err)
			writeErrorWithRequestID(w, http.StatusBadRequest, "Ошибка загрузки файла: "+err.Error(), requestID)
			return
		}
		parseDuration := time.Since(parseStart)
		log.Printf("[%s] [PREVIEW] ParseMultipartForm completed in %v", requestID, parseDuration)

		log.Printf("[%s] [PREVIEW] Getting file from form field 'file'", requestID)
		file, header, err := r.FormFile("file")
		if err != nil {
			log.Printf("[%s] [PREVIEW] ERROR: File not found in form: %v", requestID, err)
			writeErrorWithRequestID(w, http.StatusBadRequest, "Файл не найден в форме (file)", requestID)
			return
		}
		file.Close()

		ftype := r.FormValue("fileType")
		log.Printf("[%s] [PREVIEW] File info - Name: %s, Size: %d, FileType: %s", requestID, header.Filename, header.Size, ftype)

		log.Printf("[%s] [PREVIEW] Calling PreviewService.FromFile", requestID)
		res, err := h.PreviewSvc.FromFile(header, ftype)
		if err != nil {
			status := http.StatusBadRequest
			if !isUserInputError(err) {
				status = http.StatusInternalServerError
			}
			log.Printf("[%s] [PREVIEW] ERROR: PreviewService.FromFile failed: %v (status: %d)", requestID, err, status)
			writeErrorWithRequestID(w, status, err.Error(), requestID)
			return
		}

		log.Printf("[%s] [PREVIEW] SUCCESS: File processed - Columns: %d, Rows: %d", requestID, len(res.Columns), len(res.Rows))
		writeJSON(w, http.StatusOK, map[string]any{
			"columns":  res.Columns,
			"rows":     res.Rows,
			"rowCount": res.RowCount,
		})
		return
	}

	// JSON path (e.g., pg)
	log.Printf("[%s] [PREVIEW] Processing JSON request (PostgreSQL)", requestID)
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	var payload struct {
		SourceType string          `json:"sourceType"`
		Source     json.RawMessage `json:"source"`
	}

	log.Printf("[%s] [PREVIEW] Decoding JSON payload", requestID)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&payload); err != nil {
		log.Printf("[%s] [PREVIEW] ERROR: Failed to decode JSON payload: %v", requestID, err)
		writeErrorWithRequestID(w, http.StatusBadRequest, "Некорректный JSON: "+err.Error(), requestID)
		return
	}

	log.Printf("[%s] [PREVIEW] Payload decoded - SourceType: %s", requestID, payload.SourceType)
	if payload.SourceType != "postgresql" {
		log.Printf("[%s] [PREVIEW] ERROR: Unsupported sourceType: %s", requestID, payload.SourceType)
		writeErrorWithRequestID(w, http.StatusBadRequest, "Поддерживается только postgresql в JSON режиме", requestID)
		return
	}

	var _pg domain.PipelineSourcePG
	log.Printf("[%s] [PREVIEW] Unmarshaling PostgreSQL source parameters", requestID)
	if err := json.Unmarshal(payload.Source, &_pg); err != nil {
		log.Printf("[%s] [PREVIEW] ERROR: Failed to unmarshal PG source: %v", requestID, err)
		writeErrorWithRequestID(w, http.StatusBadRequest, "Некорректные параметры PG: "+err.Error(), requestID)
		return
	}

	log.Printf("[%s] [PREVIEW] PG params - DSN present: %t, Host present: %t", requestID, _pg.DSN != nil, _pg.Host != nil)
	log.Printf("[%s] [PREVIEW] Calling PreviewService.FromPG", requestID)
	res, err := h.PreviewSvc.FromPG(map[string]any{"dsn": _pg.DSN, "host": _pg.Host})
	if err != nil {
		log.Printf("[%s] [PREVIEW] ERROR: PreviewService.FromPG failed: %v", requestID, err)
		writeErrorWithRequestID(w, http.StatusInternalServerError, err.Error(), requestID)
		return
	}

	log.Printf("[%s] [PREVIEW] SUCCESS: PG processed - Columns: %d, Rows: %d", requestID, len(res.Columns), len(res.Rows))
	writeJSON(w, http.StatusOK, map[string]any{
		"columns":  res.Columns,
		"rows":     res.Rows,
		"rowCount": res.RowCount,
	})
}

func (h *HTTPHandlers) Analyze(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := fmt.Sprintf("analyze-%d", start.UnixNano())
	log.Printf("[%s] [ANALYZE] Starting request from %s", requestID, r.RemoteAddr)
	log.Printf("[%s] [ANALYZE] Content-Type: %s", requestID, r.Header.Get("Content-Type"))
	log.Printf("[%s] [ANALYZE] Content-Length: %s", requestID, r.Header.Get("Content-Length"))

	defer func() {
		duration := time.Since(start)
		log.Printf("[%s] [ANALYZE] Request completed in %v", requestID, duration)
	}()

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	var req domain.AnalyzeRequest
	log.Printf("[%s] [ANALYZE] Decoding analyze request", requestID)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		log.Printf("[%s] [ANALYZE] ERROR: Failed to decode request: %v", requestID, err)
		writeErrorWithRequestID(w, http.StatusBadRequest, "Некорректные данные предпросмотра: "+err.Error(), requestID)
		return
	}

	log.Printf("[%s] [ANALYZE] Request decoded - Preview columns: %d, rows: %d", requestID, len(req.Preview.Columns), len(req.Preview.Rows))
	log.Printf("[%s] [ANALYZE] Validating analyze request", requestID)
	if err := validateAnalyze(req); err != nil {
		log.Printf("[%s] [ANALYZE] ERROR: Validation failed: %v", requestID, err)
		writeErrorWithRequestID(w, http.StatusBadRequest, err.Error(), requestID)
		return
	}

	// Прокидываем preview в сервис анализа
	log.Printf("[%s] [ANALYZE] Marshaling preview data for service", requestID)
	previewRaw, _ := json.Marshal(req.Preview)
	log.Printf("[%s] [ANALYZE] Preview data size: %d bytes", requestID, len(previewRaw))

	log.Printf("[%s] [ANALYZE] Starting analyze service", requestID)
	id, err := h.AnalyzeSvc.StartAnalyze(previewRaw)
	if err != nil {
		log.Printf("[%s] [ANALYZE] ERROR: Failed to start analyze: %v", requestID, err)
		writeErrorWithRequestID(w, http.StatusInternalServerError, "Ошибка запуска анализа: "+err.Error(), requestID)
		return
	}

	log.Printf("[%s] [ANALYZE] SUCCESS: Analyze started with job ID: %s", requestID, id)
	writeJSONWithRequestID(w, http.StatusOK, domain.AnalyzeResponse{JobID: id}, requestID)
}

func (h *HTTPHandlers) CreatePipeline(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := fmt.Sprintf("pipeline-%d", start.UnixNano())
	log.Printf("[%s] [PIPELINE] Starting request from %s", requestID, r.RemoteAddr)
	log.Printf("[%s] [PIPELINE] Content-Type: %s", requestID, r.Header.Get("Content-Type"))
	log.Printf("[%s] [PIPELINE] Content-Length: %s", requestID, r.Header.Get("Content-Length"))

	defer func() {
		duration := time.Since(start)
		log.Printf("[%s] [PIPELINE] Request completed in %v", requestID, duration)
	}()

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	var req domain.CreatePipelineRequest
	log.Printf("[%s] [PIPELINE] Decoding pipeline request", requestID)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		log.Printf("[%s] [PIPELINE] ERROR: Failed to decode request: %v", requestID, err)
		writeErrorWithRequestID(w, http.StatusBadRequest, "Некорректные параметры: "+err.Error(), requestID)
		return
	}

	log.Printf("[%s] [PIPELINE] Request decoded - SourceType: %s", requestID, req.SourceType)
	log.Printf("[%s] [PIPELINE] Validating pipeline request", requestID)
	if err := validateCreatePipeline(req); err != nil {
		log.Printf("[%s] [PIPELINE] ERROR: Validation failed: %v", requestID, err)
		writeErrorWithRequestID(w, http.StatusBadRequest, err.Error(), requestID)
		return
	}

	log.Printf("[%s] [PIPELINE] Marshaling request for service", requestID)
	raw, _ := json.Marshal(req)
	log.Printf("[%s] [PIPELINE] Request data size: %d bytes", requestID, len(raw))

	log.Printf("[%s] [PIPELINE] Creating pipeline via service", requestID)
	id, err := h.PipelineSvc.CreatePipeline(raw)
	if err != nil {
		log.Printf("[%s] [PIPELINE] ERROR: Failed to create pipeline: %v", requestID, err)
		writeErrorWithRequestID(w, http.StatusInternalServerError, "Ошибка создания пайплайна: "+err.Error(), requestID)
		return
	}

	log.Printf("[%s] [PIPELINE] SUCCESS: Pipeline created with ID: %s", requestID, id)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(domain.CreatePipelineResponse{ID: id})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONWithRequestID(w http.ResponseWriter, status int, v any, requestID string) {
	log.Printf("[%s] [RESPONSE] Sending JSON response with status %d", requestID, status)
	writeJSON(w, status, v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func writeErrorWithRequestID(w http.ResponseWriter, status int, msg string, requestID string) {
	log.Printf("[%s] [RESPONSE] Sending error response - Status: %d, Message: %s", requestID, status, msg)
	writeJSON(w, status, map[string]any{
		"error": fmt.Sprintf("HTTP %d", status),
		"errorObject": map[string]string{
			"message":   msg,
			"requestId": requestID,
			"timestamp": time.Now().Format(time.RFC3339),
		},
	})
}

// --- validation helpers ---
func validateAnalyze(req domain.AnalyzeRequest) error {
	log.Printf("[VALIDATION] [ANALYZE] Validating analyze request")
	log.Printf("[VALIDATION] [ANALYZE] Preview columns count: %d", len(req.Preview.Columns))
	log.Printf("[VALIDATION] [ANALYZE] Preview rows count: %d", len(req.Preview.Rows))
	log.Printf("[VALIDATION] [ANALYZE] Preview rowCount field: %d", req.Preview.RowCount)

	if len(req.Preview.Columns) == 0 {
		log.Printf("[VALIDATION] [ANALYZE] ERROR: No columns provided")
		return errors.New("Некорректные данные предпросмотра: не заданы колонки")
	}
	if req.Preview.RowCount < 0 {
		log.Printf("[VALIDATION] [ANALYZE] ERROR: Negative rowCount: %d", req.Preview.RowCount)
		return errors.New("Некорректные данные предпросмотра: оотрицательный rowCount")
	}
	if len(req.Preview.Rows) > 100 {
		log.Printf("[VALIDATION] [ANALYZE] ERROR: Too many rows: %d (max 100)", len(req.Preview.Rows))
		return errors.New("Слишком много строк предпросмотра: максимум 100")
	}

	log.Printf("[VALIDATION] [ANALYZE] SUCCESS: Validation passed")
	return nil
}

func validateCreatePipeline(req domain.CreatePipelineRequest) error {
	log.Printf("[VALIDATION] [PIPELINE] Validating create pipeline request")
	log.Printf("[VALIDATION] [PIPELINE] SourceType: %s", req.SourceType)
	log.Printf("[VALIDATION] [PIPELINE] Schedule.Cron: %s", req.Schedule.Cron)
	log.Printf("[VALIDATION] [PIPELINE] Schedule.IncrementalMode: %s", req.Schedule.IncrementalMode)
	log.Printf("[VALIDATION] [PIPELINE] Schedule.IncrementalColumn: %s", req.Schedule.IncrementalColumn)

	switch req.SourceType {
	case "csv", "json", "xml", "postgresql":
		log.Printf("[VALIDATION] [PIPELINE] SourceType validation passed")
	default:
		log.Printf("[VALIDATION] [PIPELINE] ERROR: Invalid sourceType: %s", req.SourceType)
		return errors.New("Некорректные параметры: sourceType")
	}

	log.Printf("[VALIDATION] [PIPELINE] Validating source parameters")
	if err := validateSource(req.SourceType, req.Source); err != nil {
		log.Printf("[VALIDATION] [PIPELINE] ERROR: Source validation failed: %v", err)
		return err
	}

	log.Printf("[VALIDATION] [PIPELINE] Validating DDL parameters")
	log.Printf("[VALIDATION] [PIPELINE] DDL.ClickHouse present: %t", req.DDL.ClickHouse != "")
	log.Printf("[VALIDATION] [PIPELINE] DDL.PostgreSQL present: %t", req.DDL.PostgreSQL != "")
	log.Printf("[VALIDATION] [PIPELINE] DDL.HDFS present: %t", req.DDL.HDFS != "")
	if req.DDL.ClickHouse == "" || req.DDL.PostgreSQL == "" || req.DDL.HDFS == "" {
		log.Printf("[VALIDATION] [PIPELINE] ERROR: Missing DDL parameters")
		return errors.New("Некорректные параметры: ddl.clickhouse/postgresql/hdfs обязательны")
	}

	if req.Schedule.Cron == "" {
		log.Printf("[VALIDATION] [PIPELINE] ERROR: Missing cron schedule")
		return errors.New("Некорректные параметры: schedule.cron обязателен")
	}

	switch req.Schedule.IncrementalMode {
	case "none", "updated_at", "id":
		log.Printf("[VALIDATION] [PIPELINE] IncrementalMode validation passed")
	default:
		log.Printf("[VALIDATION] [PIPELINE] ERROR: Invalid incrementalMode: %s", req.Schedule.IncrementalMode)
		return errors.New("Некорректные параметры: schedule.incrementalMode")
	}

	if req.Schedule.IncrementalMode != "none" && req.Schedule.IncrementalColumn == "" {
		log.Printf("[VALIDATION] [PIPELINE] ERROR: Missing incrementalColumn for mode: %s", req.Schedule.IncrementalMode)
		return errors.New("Некорректные параметры: schedule.incrementalColumn обязателен при инкрементальном режиме")
	}

	log.Printf("[VALIDATION] [PIPELINE] SUCCESS: Validation passed")
	return nil
}

func validateSource(sourceType string, raw json.RawMessage) error {
	log.Printf("[VALIDATION] [SOURCE] Validating source for type: %s", sourceType)
	log.Printf("[VALIDATION] [SOURCE] Raw source data size: %d bytes", len(raw))

	var probe struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		log.Printf("[VALIDATION] [SOURCE] ERROR: Failed to unmarshal probe: %v", err)
		return errors.New("Некорректные параметры: source")
	}

	log.Printf("[VALIDATION] [SOURCE] Source kind: %s", probe.Kind)
	switch probe.Kind {
	case "file":
		log.Printf("[VALIDATION] [SOURCE] Validating file source")
		var f domain.PipelineSourceFile
		if err := json.Unmarshal(raw, &f); err != nil {
			log.Printf("[VALIDATION] [SOURCE] ERROR: Failed to unmarshal file source: %v", err)
			return errors.New("Некорректные параметры: source.file")
		}

		log.Printf("[VALIDATION] [SOURCE] File type: %s, PathOrURL: %s", f.Type, f.PathOrURL)
		switch f.Type {
		case "csv", "json", "xml":
			log.Printf("[VALIDATION] [SOURCE] File type validation passed")
		default:
			log.Printf("[VALIDATION] [SOURCE] ERROR: Invalid file type: %s", f.Type)
			return errors.New("Некорректные параметры: source.file.type")
		}

		// PathOrURL может быть пустым, если файл уже был загружен через preview
		if f.PathOrURL == "" {
			log.Printf("[VALIDATION] [SOURCE] WARNING: Empty pathOrUrl - assuming file was uploaded via preview")
		} else {
			log.Printf("[VALIDATION] [SOURCE] PathOrURL provided: %s", f.PathOrURL)
		}

		if sourceType != "postgresql" && sourceType != f.Type {
			log.Printf("[VALIDATION] [SOURCE] ERROR: SourceType mismatch - expected: %s, got: %s", f.Type, sourceType)
			return fmt.Errorf("Некорректные параметры: sourceType должен совпадать с типом файла")
		}

		log.Printf("[VALIDATION] [SOURCE] File source validation passed")
		return nil

	case "pg":
		log.Printf("[VALIDATION] [SOURCE] Validating PostgreSQL source")
		var pg domain.PipelineSourcePG
		if err := json.Unmarshal(raw, &pg); err != nil {
			log.Printf("[VALIDATION] [SOURCE] ERROR: Failed to unmarshal PG source: %v", err)
			return errors.New("Некорректные параметры: source.pg")
		}

		log.Printf("[VALIDATION] [SOURCE] PG DSN present: %t, Host present: %t", pg.DSN != nil && *pg.DSN != "", pg.Host != nil && *pg.Host != "")
		if (pg.DSN == nil || *pg.DSN == "") && (pg.Host == nil || *pg.Host == "") {
			log.Printf("[VALIDATION] [SOURCE] ERROR: Neither DSN nor Host provided")
			return errors.New("Некорректные параметры: pg.dsn или pg.host обязателен")
		}

		log.Printf("[VALIDATION] [SOURCE] PostgreSQL source validation passed")
		return nil

	default:
		log.Printf("[VALIDATION] [SOURCE] ERROR: Unknown source kind: %s", probe.Kind)
		return errors.New("Некорректные параметры: source.kind")
	}
}

func isUserInputError(err error) bool {
	s := err.Error()
	return strings.Contains(s, "unsupported") || strings.Contains(s, "expected") || strings.Contains(s, "Некоррект")
}

// GetPipelines возвращает список всех пайплайнов из Airflow
func (h *HTTPHandlers) GetPipelines(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := fmt.Sprintf("get-pipelines-%d", start.UnixNano())
	log.Printf("[%s] [GET_PIPELINES] Starting request from %s", requestID, r.RemoteAddr)

	defer func() {
		duration := time.Since(start)
		log.Printf("[%s] [GET_PIPELINES] Request completed in %v", requestID, duration)
	}()

	log.Printf("[%s] [GET_PIPELINES] Fetching DAGs from Airflow", requestID)
	dags, err := h.AirflowSvc.GetDAGs()
	if err != nil {
		log.Printf("[%s] [GET_PIPELINES] ERROR: Failed to get DAGs: %v", requestID, err)
		writeErrorWithRequestID(w, http.StatusInternalServerError, "Ошибка получения пайплайнов: "+err.Error(), requestID)
		return
	}

	// Фильтруем только наши сгенерированные DAG (начинающиеся с "pipe_")
	var pipelines []map[string]interface{}
	for _, dag := range dags {
		if strings.HasPrefix(dag.DagID, "pipe_") {
			log.Printf("[%s] [GET_PIPELINES] Found pipeline DAG: %s", requestID, dag.DagID)

			// Получаем информацию о последних запусках
			runs, err := h.AirflowSvc.GetDAGRuns(dag.DagID)
			if err != nil {
				log.Printf("[%s] [GET_PIPELINES] WARNING: Failed to get runs for %s: %v", requestID, dag.DagID, err)
				runs = []usecase.AirflowDAGRun{} // Пустой массив если не удалось получить
			}

			var lastRun *usecase.AirflowDAGRun
			if len(runs) > 0 {
				lastRun = &runs[0] // Первый элемент - последний запуск
			}

			pipeline := map[string]interface{}{
				"id":          dag.DagID,
				"name":        dag.DagID,
				"description": dag.Description,
				"is_paused":   dag.IsPaused,
				"is_active":   dag.IsActive,
				"last_parsed": dag.LastParsed,
				"file_loc":    dag.FileLoc,
				"runs_count":  len(runs),
			}

			if lastRun != nil {
				pipeline["last_run"] = map[string]interface{}{
					"dag_run_id":       lastRun.DagRunID,
					"state":            lastRun.State,
					"execution_date":   lastRun.ExecutionDate,
					"start_date":       lastRun.StartDate,
					"end_date":         lastRun.EndDate,
					"external_trigger": lastRun.ExternalTrigger,
				}
			}

			pipelines = append(pipelines, pipeline)
		}
	}

	log.Printf("[%s] [GET_PIPELINES] SUCCESS: Found %d pipelines", requestID, len(pipelines))
	writeJSONWithRequestID(w, http.StatusOK, map[string]interface{}{
		"pipelines": pipelines,
		"total":     len(pipelines),
	}, requestID)
}

// GetPipeline возвращает информацию о конкретном пайплайне
func (h *HTTPHandlers) GetPipeline(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := fmt.Sprintf("get-pipeline-%d", start.UnixNano())

	// Извлекаем ID пайплайна из URL
	pipelineID := r.URL.Query().Get("id")
	if pipelineID == "" {
		writeErrorWithRequestID(w, http.StatusBadRequest, "Параметр 'id' обязателен", requestID)
		return
	}

	log.Printf("[%s] [GET_PIPELINE] Starting request for pipeline: %s", requestID, pipelineID)

	defer func() {
		duration := time.Since(start)
		log.Printf("[%s] [GET_PIPELINE] Request completed in %v", requestID, duration)
	}()

	// Получаем информацию о DAG
	log.Printf("[%s] [GET_PIPELINE] Fetching DAG info from Airflow", requestID)
	dag, err := h.AirflowSvc.GetDAG(pipelineID)
	if err != nil {
		log.Printf("[%s] [GET_PIPELINE] ERROR: Failed to get DAG: %v", requestID, err)
		status := http.StatusNotFound
		if !strings.Contains(err.Error(), "not found") {
			status = http.StatusInternalServerError
		}
		writeErrorWithRequestID(w, status, "Ошибка получения пайплайна: "+err.Error(), requestID)
		return
	}

	// Получаем историю запусков
	log.Printf("[%s] [GET_PIPELINE] Fetching DAG runs", requestID)
	runs, err := h.AirflowSvc.GetDAGRuns(pipelineID)
	if err != nil {
		log.Printf("[%s] [GET_PIPELINE] WARNING: Failed to get runs: %v", requestID, err)
		runs = []usecase.AirflowDAGRun{} // Пустой массив если не удалось получить
	}

	pipeline := map[string]interface{}{
		"id":          dag.DagID,
		"name":        dag.DagID,
		"description": dag.Description,
		"is_paused":   dag.IsPaused,
		"is_active":   dag.IsActive,
		"last_parsed": dag.LastParsed,
		"file_loc":    dag.FileLoc,
		"runs":        runs,
		"runs_count":  len(runs),
	}

	log.Printf("[%s] [GET_PIPELINE] SUCCESS: Retrieved pipeline %s with %d runs", requestID, pipelineID, len(runs))
	writeJSONWithRequestID(w, http.StatusOK, pipeline, requestID)
}

// TriggerPipeline запускает выполнение пайплайна
func (h *HTTPHandlers) TriggerPipeline(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := fmt.Sprintf("trigger-pipeline-%d", start.UnixNano())
	log.Printf("[%s] [TRIGGER_PIPELINE] Starting request from %s", requestID, r.RemoteAddr)

	defer func() {
		duration := time.Since(start)
		log.Printf("[%s] [TRIGGER_PIPELINE] Request completed in %v", requestID, duration)
	}()

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	var req struct {
		PipelineID string                 `json:"pipeline_id"`
		Config     map[string]interface{} `json:"config,omitempty"`
	}

	log.Printf("[%s] [TRIGGER_PIPELINE] Decoding request", requestID)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		log.Printf("[%s] [TRIGGER_PIPELINE] ERROR: Failed to decode request: %v", requestID, err)
		writeErrorWithRequestID(w, http.StatusBadRequest, "Некорректные данные запроса: "+err.Error(), requestID)
		return
	}

	if req.PipelineID == "" {
		log.Printf("[%s] [TRIGGER_PIPELINE] ERROR: Missing pipeline_id", requestID)
		writeErrorWithRequestID(w, http.StatusBadRequest, "Параметр 'pipeline_id' обязателен", requestID)
		return
	}

	log.Printf("[%s] [TRIGGER_PIPELINE] Triggering pipeline: %s", requestID, req.PipelineID)
	dagRun, err := h.AirflowSvc.TriggerDAG(req.PipelineID, req.Config)
	if err != nil {
		log.Printf("[%s] [TRIGGER_PIPELINE] ERROR: Failed to trigger DAG: %v", requestID, err)
		writeErrorWithRequestID(w, http.StatusInternalServerError, "Ошибка запуска пайплайна: "+err.Error(), requestID)
		return
	}

	log.Printf("[%s] [TRIGGER_PIPELINE] SUCCESS: Pipeline triggered, run ID: %s", requestID, dagRun.DagRunID)
	writeJSONWithRequestID(w, http.StatusOK, map[string]interface{}{
		"message":     "Пайплайн успешно запущен",
		"pipeline_id": req.PipelineID,
		"run_id":      dagRun.DagRunID,
		"state":       dagRun.State,
	}, requestID)
}

// GetDatabaseStatus возвращает статус подключений к базам данных
func (h *HTTPHandlers) GetDatabaseStatus(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := fmt.Sprintf("db-status-%d", start.UnixNano())
	log.Printf("[%s] [DB_STATUS] Starting request from %s", requestID, r.RemoteAddr)

	defer func() {
		duration := time.Since(start)
		log.Printf("[%s] [DB_STATUS] Request completed in %v", requestID, duration)
	}()

	log.Printf("[%s] [DB_STATUS] Testing database connections", requestID)
	connections := h.DatabaseSvc.TestConnections()

	// Получаем информацию о таблицах PostgreSQL если подключение работает
	var tables []usecase.TableInfo
	for _, conn := range connections {
		if conn.Type == "postgresql" && conn.Connected {
			log.Printf("[%s] [DB_STATUS] Getting PostgreSQL tables", requestID)
			pgTables, err := h.DatabaseSvc.GetPostgreSQLTables()
			if err != nil {
				log.Printf("[%s] [DB_STATUS] WARNING: Failed to get PostgreSQL tables: %v", requestID, err)
			} else {
				tables = pgTables
				log.Printf("[%s] [DB_STATUS] Found %d PostgreSQL tables", requestID, len(tables))
			}
			break
		}
	}

	log.Printf("[%s] [DB_STATUS] SUCCESS: Database status retrieved", requestID)
	writeJSONWithRequestID(w, http.StatusOK, map[string]interface{}{
		"connections": connections,
		"tables":      tables,
		"timestamp":   time.Now().Format(time.RFC3339),
	}, requestID)
}

// InitializeSampleData создает образцы данных в базах
func (h *HTTPHandlers) InitializeSampleData(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	requestID := fmt.Sprintf("init-data-%d", start.UnixNano())
	log.Printf("[%s] [INIT_DATA] Starting request from %s", requestID, r.RemoteAddr)

	defer func() {
		duration := time.Since(start)
		log.Printf("[%s] [INIT_DATA] Request completed in %v", requestID, duration)
	}()

	log.Printf("[%s] [INIT_DATA] Creating sample data", requestID)
	if err := h.DatabaseSvc.CreateSampleData(); err != nil {
		log.Printf("[%s] [INIT_DATA] ERROR: Failed to create sample data: %v", requestID, err)
		writeErrorWithRequestID(w, http.StatusInternalServerError, "Ошибка создания образцов данных: "+err.Error(), requestID)
		return
	}

	log.Printf("[%s] [INIT_DATA] SUCCESS: Sample data created", requestID)
	writeJSONWithRequestID(w, http.StatusOK, map[string]interface{}{
		"message":   "Образцы данных успешно созданы",
		"timestamp": time.Now().Format(time.RFC3339),
	}, requestID)
}
