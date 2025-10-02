package usecase

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"strings"
)

type PreviewResult struct {
	Columns  []struct{ Name, Type string }
	Rows     []map[string]any
	RowCount int
}

type PreviewService struct{}

func (s *PreviewService) FromFile(fileHeader *multipart.FileHeader, typ string) (PreviewResult, error) {
	log.Printf("[PREVIEW_SERVICE] [FROM_FILE] Processing file: %s, type: %s, size: %d", fileHeader.Filename, typ, fileHeader.Size)

	f, err := fileHeader.Open()
	if err != nil {
		log.Printf("[PREVIEW_SERVICE] [FROM_FILE] ERROR: Failed to open file: %v", err)
		return PreviewResult{}, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	normalizedType := strings.ToLower(typ)
	log.Printf("[PREVIEW_SERVICE] [FROM_FILE] Normalized file type: %s", normalizedType)

	switch normalizedType {
	case "csv":
		log.Printf("[PREVIEW_SERVICE] [FROM_FILE] Processing as CSV")
		result, err := previewCSV(f)
		if err != nil {
			log.Printf("[PREVIEW_SERVICE] [FROM_FILE] ERROR: CSV processing failed: %v", err)
			return PreviewResult{}, fmt.Errorf("CSV processing failed: %w", err)
		}
		log.Printf("[PREVIEW_SERVICE] [FROM_FILE] SUCCESS: CSV processed - columns: %d, rows: %d", len(result.Columns), len(result.Rows))
		return result, nil

	case "json":
		log.Printf("[PREVIEW_SERVICE] [FROM_FILE] Processing as JSON")
		result, err := previewJSON(f)
		if err != nil {
			log.Printf("[PREVIEW_SERVICE] [FROM_FILE] ERROR: JSON processing failed: %v", err)
			return PreviewResult{}, fmt.Errorf("JSON processing failed: %w", err)
		}
		log.Printf("[PREVIEW_SERVICE] [FROM_FILE] SUCCESS: JSON processed - columns: %d, rows: %d", len(result.Columns), len(result.Rows))
		return result, nil

	case "xml":
		log.Printf("[PREVIEW_SERVICE] [FROM_FILE] Processing as XML")
		result, err := previewXML(f)
		if err != nil {
			log.Printf("[PREVIEW_SERVICE] [FROM_FILE] ERROR: XML processing failed: %v", err)
			return PreviewResult{}, fmt.Errorf("XML processing failed: %w", err)
		}
		log.Printf("[PREVIEW_SERVICE] [FROM_FILE] SUCCESS: XML processed - columns: %d, rows: %d", len(result.Columns), len(result.Rows))
		return result, nil

	default:
		log.Printf("[PREVIEW_SERVICE] [FROM_FILE] ERROR: Unsupported file type: %s", normalizedType)
		return PreviewResult{}, errors.New("unsupported file type")
	}
}

func (s *PreviewService) FromPG(params map[string]any) (PreviewResult, error) {
	log.Printf("[PREVIEW_SERVICE] [FROM_PG] Processing PostgreSQL preview request")
	log.Printf("[PREVIEW_SERVICE] [FROM_PG] Parameters: %+v", params)

	// Извлекаем параметры подключения
	var dsn, host string

	if dsnParam, ok := params["dsn"]; ok && dsnParam != nil {
		if dsnStr, ok := dsnParam.(*string); ok && dsnStr != nil {
			dsn = *dsnStr
		}
	}

	if hostParam, ok := params["host"]; ok && hostParam != nil {
		if hostStr, ok := hostParam.(*string); ok && hostStr != nil {
			host = *hostStr
		}
	}

	log.Printf("[PREVIEW_SERVICE] [FROM_PG] Connection params - DSN: %s, Host: %s",
		maskSensitiveInfo(dsn), host)

	// Пока что возвращаем mock данные с реалистичной структурой
	// TODO: Реализовать реальное подключение к PostgreSQL
	log.Printf("[PREVIEW_SERVICE] [FROM_PG] WARNING: Using mock data - PostgreSQL connection not implemented yet")

	result := PreviewResult{
		Columns: []struct{ Name, Type string }{
			{Name: "id", Type: "integer"},
			{Name: "name", Type: "string"},
			{Name: "email", Type: "string"},
			{Name: "created_at", Type: "datetime"},
			{Name: "is_active", Type: "boolean"},
		},
		Rows: []map[string]any{
			{"id": 1, "name": "John Doe", "email": "john@example.com", "created_at": "2023-01-01T10:00:00Z", "is_active": true},
			{"id": 2, "name": "Jane Smith", "email": "jane@example.com", "created_at": "2023-01-02T11:00:00Z", "is_active": true},
			{"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "created_at": "2023-01-03T12:00:00Z", "is_active": false},
		},
		RowCount: 3,
	}

	log.Printf("[PREVIEW_SERVICE] [FROM_PG] SUCCESS: Returning mock preview - columns: %d, rows: %d",
		len(result.Columns), len(result.Rows))
	return result, nil
}

// maskSensitiveInfo маскирует чувствительную информацию в логах
func maskSensitiveInfo(dsn string) string {
	if dsn == "" {
		return ""
	}
	// Простая маскировка - показываем только первые и последние символы
	if len(dsn) <= 8 {
		return "***"
	}
	return dsn[:4] + "***" + dsn[len(dsn)-4:]
}

func previewCSV(r io.Reader) (PreviewResult, error) {
	log.Printf("[PREVIEW_CSV] Starting CSV preview processing")

	br := bufio.NewReader(r)
	// Peek первую строку для автоопределения разделителя
	log.Printf("[PREVIEW_CSV] Detecting CSV separator")
	line, _ := br.Peek(4096)
	sep := detectCSVSeparator(string(line))
	log.Printf("[PREVIEW_CSV] Detected separator: %q", sep)

	cr := csv.NewReader(br)
	cr.FieldsPerRecord = -1
	cr.Comma = sep

	log.Printf("[PREVIEW_CSV] Reading CSV header")
	head, err := cr.Read()
	if err != nil {
		log.Printf("[PREVIEW_CSV] ERROR: Failed to read header: %v", err)
		return PreviewResult{}, fmt.Errorf("failed to read CSV header: %w", err)
	}

	log.Printf("[PREVIEW_CSV] Header read - columns: %d", len(head))
	for i, name := range head {
		log.Printf("[PREVIEW_CSV] Column %d: %s", i, name)
	}

	cols := make([]struct{ Name, Type string }, len(head))
	for i, name := range head {
		cols[i] = struct{ Name, Type string }{Name: name, Type: "string"}
	}

	log.Printf("[PREVIEW_CSV] Reading data rows (max 100)")
	rows := make([]map[string]any, 0, 100)
	for len(rows) < 100 {
		rec, err := cr.Read()
		if err == io.EOF {
			log.Printf("[PREVIEW_CSV] Reached end of file at row %d", len(rows))
			break
		}
		if err != nil {
			log.Printf("[PREVIEW_CSV] ERROR: Failed to read row %d: %v", len(rows), err)
			return PreviewResult{}, fmt.Errorf("failed to read CSV row %d: %w", len(rows), err)
		}

		row := make(map[string]any, len(head))
		for i, name := range head {
			val := ""
			if i < len(rec) {
				val = rec[i]
			}
			row[name] = val
		}
		rows = append(rows, row)
	}

	log.Printf("[PREVIEW_CSV] SUCCESS: Processed %d rows with %d columns", len(rows), len(cols))
	return PreviewResult{Columns: cols, Rows: rows, RowCount: len(rows)}, nil
}

func detectCSVSeparator(sample string) rune {
	// смотрим до первой новой строки
	if idx := strings.IndexByte(sample, '\n'); idx >= 0 {
		sample = sample[:idx]
	}
	candidates := []rune{',', ';', '\t', '|'}
	best := ','
	bestCount := -1
	for _, c := range candidates {
		cnt := strings.Count(sample, string(c))
		if cnt > bestCount {
			bestCount = cnt
			best = c
		}
	}
	return best
}

func previewJSON(r io.Reader) (PreviewResult, error) {
	// Поддержка: массив объектов, одиночный объект, объект с массивом внутри, NDJSON
	// Читаем всё (для предпросмотра это приемлемо)
	data, err := ioutil.ReadAll(bufio.NewReader(r))
	if err != nil {
		return PreviewResult{}, err
	}

	trim := strings.TrimSpace(string(data))
	rows := make([]map[string]any, 0, 100)

	// 1) Корневой массив
	if strings.HasPrefix(trim, "[") {
		var arr []map[string]any
		if err := json.Unmarshal(data, &arr); err != nil {
			return PreviewResult{}, err
		}
		for i := 0; i < len(arr) && len(rows) < 100; i++ {
			rows = append(rows, arr[i])
		}
		return buildJSONPreview(rows), nil
	}

	// 2) Корневой объект
	if strings.HasPrefix(trim, "{") {
		var obj map[string]any
		if err := json.Unmarshal(data, &obj); err != nil {
			return PreviewResult{}, err
		}
		// Попробуем найти первое поле-массив объектов
		for _, v := range obj {
			if arr, ok := v.([]any); ok {
				for _, it := range arr {
					if m, ok := it.(map[string]any); ok {
						rows = append(rows, m)
						if len(rows) >= 100 {
							break
						}
					}
				}
				if len(rows) > 0 {
					return buildJSONPreview(rows), nil
				}
			}
		}
		// Иначе трактуем сам объект как строку предпросмотра
		rows = append(rows, obj)
		return buildJSONPreview(rows), nil
	}

	// 3) NDJSON: каждая строка — объект
	scanner := bufio.NewScanner(strings.NewReader(trim))
	for scanner.Scan() {
		var m map[string]any
		if err := json.Unmarshal([]byte(scanner.Text()), &m); err == nil {
			rows = append(rows, m)
			if len(rows) >= 100 {
				break
			}
		}
	}
	if len(rows) == 0 {
		return PreviewResult{}, errors.New("unsupported json structure")
	}
	return buildJSONPreview(rows), nil
}

func buildJSONPreview(rows []map[string]any) PreviewResult {
	colsMap := map[string]string{}
	for _, r := range rows {
		for k, v := range r {
			if _, ok := colsMap[k]; !ok {
				colsMap[k] = inferJSONType(v)
			}
		}
	}
	cols := make([]struct{ Name, Type string }, 0, len(colsMap))
	for k, t := range colsMap {
		cols = append(cols, struct{ Name, Type string }{Name: k, Type: t})
	}
	return PreviewResult{Columns: cols, Rows: rows, RowCount: len(rows)}
}

func inferJSONType(v any) string {
	switch s := v.(type) {
	case string:
		// Простой хинт: ISO8601 часто содержит '-' и ':'
		if len(s) >= 19 && strings.Count(s, "-") >= 2 && strings.Count(s, ":") >= 2 {
			return "datetime"
		}
		return "string"
	case float64:
		return "number"
	case bool:
		return "bool"
	case nil:
		return "string"
	default:
		return "string"
	}
}

// previewXML поддерживает простой плоский формат: <root><row><a>1</a><b>2</b></row>...</root>
// Ищет первый повторяющийся узел под корнем и извлекает его дочерние элементы как колонки
func previewXML(r io.Reader) (PreviewResult, error) {
	dec := xml.NewDecoder(bufio.NewReader(r))
	depth := 0
	rootOpened := false
	var rowName string
	rows := make([]map[string]any, 0, 100)
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return PreviewResult{}, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			depth++
			if !rootOpened {
				rootOpened = true
				continue
			}
			if depth == 2 { // прямые дети корня
				if rowName == "" {
					rowName = t.Name.Local
				}
				if t.Name.Local == rowName {
					// читаем плоские поля
					row := map[string]any{}
					// consume children until EndElement for row
					for {
						tok2, err2 := dec.Token()
						if err2 != nil {
							if err2 == io.EOF {
								break
							}
							return PreviewResult{}, err2
						}
						switch tt := tok2.(type) {
						case xml.StartElement:
							// читаем текстовое содержимое
							var text string
							if err := dec.DecodeElement(&text, &tt); err == nil {
								row[tt.Name.Local] = text
							}
						case xml.EndElement:
							if tt.Name.Local == rowName {
								rows = append(rows, row)
								goto nextToken
							}
						}
					}
				}
			}
		case xml.EndElement:
			if rootOpened && depth == 1 {
				// конец корня
				break
			}
			if depth > 0 {
				depth--
			}
		}
	nextToken:
		if len(rows) >= 100 {
			break
		}
	}
	if len(rows) == 0 {
		return PreviewResult{}, errors.New("unsupported xml structure")
	}
	return buildJSONPreview(rows), nil
}
