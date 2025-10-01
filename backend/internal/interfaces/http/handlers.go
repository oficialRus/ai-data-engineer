package httpiface

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/user/ai-data-engineer/backend/internal/config"
	domain "github.com/user/ai-data-engineer/backend/internal/domain"
	"github.com/user/ai-data-engineer/backend/internal/usecase"
)

const maxRequestBodyBytes = 1 << 20 // 1 MiB

type HTTPHandlers struct {
	AnalyzeSvc  *usecase.AnalyzeService
	PipelineSvc *usecase.PipelineService
	PreviewSvc  *usecase.PreviewService
	Cfg         config.AppConfig
}

func (h *HTTPHandlers) Preview(w http.ResponseWriter, r *http.Request) {
	// Support multipart (file) and JSON (pg)
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		// file upload
		if err := r.ParseMultipartForm(h.Cfg.Limits.MaxFileSizeBytes); err != nil {
			writeError(w, http.StatusBadRequest, "Ошибка загрузки файла: "+err.Error())
			return
		}
		file, header, err := r.FormFile("file")
		if err != nil {
			writeError(w, http.StatusBadRequest, "Файл не найден в форме (file)")
			return
		}
		file.Close()
		ftype := r.FormValue("fileType")
		res, err := h.PreviewSvc.FromFile(header, ftype)
		if err != nil {
			status := http.StatusBadRequest
			if !isUserInputError(err) {
				status = http.StatusInternalServerError
			}
			writeError(w, status, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"columns":  res.Columns,
			"rows":     res.Rows,
			"rowCount": res.RowCount,
		})
		return
	}
	// JSON path (e.g., pg)
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()
	var payload struct {
		SourceType string          `json:"sourceType"`
		Source     json.RawMessage `json:"source"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "Некорректный JSON: "+err.Error())
		return
	}
	if payload.SourceType != "postgresql" {
		writeError(w, http.StatusBadRequest, "Поддерживается только postgresql в JSON режиме")
		return
	}
	var _pg domain.PipelineSourcePG
	if err := json.Unmarshal(payload.Source, &_pg); err != nil {
		writeError(w, http.StatusBadRequest, "Некорректные параметры PG: "+err.Error())
		return
	}
	res, err := h.PreviewSvc.FromPG(map[string]any{"dsn": _pg.DSN, "host": _pg.Host})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"columns":  res.Columns,
		"rows":     res.Rows,
		"rowCount": res.RowCount,
	})
}

func (h *HTTPHandlers) Analyze(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()
	var req domain.AnalyzeRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Некорректные данные предпросмотра: "+err.Error())
		return
	}
	if err := validateAnalyze(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	id, err := h.AnalyzeSvc.StartAnalyze(nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Ошибка запуска анализа: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, domain.AnalyzeResponse{JobID: id})
}

func (h *HTTPHandlers) CreatePipeline(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()
	var req domain.CreatePipelineRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Некорректные параметры: "+err.Error())
		return
	}
	if err := validateCreatePipeline(req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	id, err := h.PipelineSvc.CreatePipeline(nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Ошибка создания пайплайна: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(domain.CreatePipelineResponse{ID: id})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// --- validation helpers ---
func validateAnalyze(req domain.AnalyzeRequest) error {
	if len(req.Preview.Columns) == 0 {
		return errors.New("Некорректные данные предпросмотра: не заданы колонки")
	}
	if req.Preview.RowCount < 0 {
		return errors.New("Некорректные данные предпросмотра: отрицательный rowCount")
	}
	if len(req.Preview.Rows) > 100 {
		return errors.New("Слишком много строк предпросмотра: максимум 100")
	}
	return nil
}

func validateCreatePipeline(req domain.CreatePipelineRequest) error {
	switch req.SourceType {
	case "csv", "json", "xml", "postgresql":
	default:
		return errors.New("Некорректные параметры: sourceType")
	}
	if err := validateSource(req.SourceType, req.Source); err != nil {
		return err
	}
	if req.DDL.ClickHouse == "" || req.DDL.PostgreSQL == "" || req.DDL.HDFS == "" {
		return errors.New("Некорректные параметры: ddl.clickhouse/postgresql/hdfs обязательны")
	}
	if req.Schedule.Cron == "" {
		return errors.New("Некорректные параметры: schedule.cron обязателен")
	}
	switch req.Schedule.IncrementalMode {
	case "none", "updated_at", "id":
	default:
		return errors.New("Некорректные параметры: schedule.incrementalMode")
	}
	if req.Schedule.IncrementalMode != "none" && req.Schedule.IncrementalColumn == "" {
		return errors.New("Некорректные параметры: schedule.incrementalColumn обязателен при инкрементальном режиме")
	}
	return nil
}

func validateSource(sourceType string, raw json.RawMessage) error {
	var probe struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return errors.New("Некорректные параметры: source")
	}
	switch probe.Kind {
	case "file":
		var f domain.PipelineSourceFile
		if err := json.Unmarshal(raw, &f); err != nil {
			return errors.New("Некорректные параметры: source.file")
		}
		switch f.Type {
		case "csv", "json", "xml":
		default:
			return errors.New("Некорректные параметры: source.file.type")
		}
		if f.PathOrURL == "" {
			return errors.New("Некорректные параметры: source.file.pathOrUrl")
		}
		if sourceType != "postgresql" && sourceType != f.Type {
			return errors.New("Некорректные параметры: sourceType должен совпадать с типом файла")
		}
		return nil
	case "pg":
		var pg domain.PipelineSourcePG
		if err := json.Unmarshal(raw, &pg); err != nil {
			return errors.New("Некорректные параметры: source.pg")
		}
		if (pg.DSN == nil || *pg.DSN == "") && (pg.Host == nil || *pg.Host == "") {
			return errors.New("Некорректные параметры: pg.dsn или pg.host обязателен")
		}
		return nil
	default:
		return errors.New("Некорректные параметры: source.kind")
	}
}

func isUserInputError(err error) bool {
	s := err.Error()
	return strings.Contains(s, "unsupported") || strings.Contains(s, "expected") || strings.Contains(s, "Некоррект")
}
