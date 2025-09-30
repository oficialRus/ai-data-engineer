package usecase

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
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
	f, err := fileHeader.Open()
	if err != nil {
		return PreviewResult{}, err
	}
	defer f.Close()
	switch strings.ToLower(typ) {
	case "csv":
		return previewCSV(f)
	case "json":
		return previewJSON(f)
	default:
		return PreviewResult{}, errors.New("unsupported file type")
	}
}

func (s *PreviewService) FromPG(params map[string]any) (PreviewResult, error) {
	// Stub: return empty preview
	return PreviewResult{Columns: []struct{ Name, Type string }{}, Rows: []map[string]any{}, RowCount: 0}, nil
}

func previewCSV(r io.Reader) (PreviewResult, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	head, err := cr.Read()
	if err != nil {
		return PreviewResult{}, err
	}
	cols := make([]struct{ Name, Type string }, len(head))
	for i, name := range head {
		cols[i] = struct{ Name, Type string }{Name: name, Type: "string"}
	}
	rows := make([]map[string]any, 0, 100)
	for len(rows) < 100 {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return PreviewResult{}, err
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
	return PreviewResult{Columns: cols, Rows: rows, RowCount: len(rows)}, nil
}

func previewJSON(r io.Reader) (PreviewResult, error) {
	// Expect NDJSON or array
	dec := json.NewDecoder(bufio.NewReader(r))
	rows := make([]map[string]any, 0, 100)
	// Try array first
	t, err := dec.Token()
	if err != nil {
		return PreviewResult{}, err
	}
	if d, ok := t.(json.Delim); ok && d.String() == "[" {
		for dec.More() && len(rows) < 100 {
			var m map[string]any
			if err := dec.Decode(&m); err != nil {
				return PreviewResult{}, err
			}
			rows = append(rows, m)
		}
		// consume ]
		_, _ = dec.Token()
	} else {
		// token belongs to first value; treat as single object stream
		var first map[string]any
		switch v := t.(type) {
		case json.Delim:
			return PreviewResult{}, errors.New("unsupported json structure")
		case string, float64, bool, nil:
			return PreviewResult{}, errors.New("expected object(s)")
		default:
			_ = v
		}
		if err := dec.Decode(&first); err != nil {
			return PreviewResult{}, err
		}
		rows = append(rows, first)
		for len(rows) < 100 {
			var m map[string]any
			if err := dec.Decode(&m); err != nil {
				break
			}
			rows = append(rows, m)
		}
	}
	colsMap := map[string]struct{}{}
	for _, r := range rows {
		for k := range r {
			colsMap[k] = struct{}{}
		}
	}
	cols := make([]struct{ Name, Type string }, 0, len(colsMap))
	for k := range colsMap {
		cols = append(cols, struct{ Name, Type string }{Name: k, Type: "any"})
	}
	return PreviewResult{Columns: cols, Rows: rows, RowCount: len(rows)}, nil
}
