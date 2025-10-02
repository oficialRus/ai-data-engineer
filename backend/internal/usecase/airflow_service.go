package usecase

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/user/ai-data-engineer/backend/internal/config"
)

type AirflowService struct {
	Cfg config.AppConfig
}

type AirflowDAG struct {
	DagID       string `json:"dag_id"`
	Description string `json:"description,omitempty"`
	IsPaused    bool   `json:"is_paused"`
	IsActive    bool   `json:"is_active"`
	LastParsed  string `json:"last_parsed,omitempty"`
	FileLoc     string `json:"fileloc,omitempty"`
}

type AirflowDAGRun struct {
	DagRunID        string `json:"dag_run_id"`
	DagID           string `json:"dag_id"`
	State           string `json:"state"`
	ExecutionDate   string `json:"execution_date"`
	StartDate       string `json:"start_date,omitempty"`
	EndDate         string `json:"end_date,omitempty"`
	ExternalTrigger bool   `json:"external_trigger"`
}

type AirflowConnection struct {
	ConnectionID string `json:"connection_id"`
	ConnType     string `json:"conn_type"`
	Host         string `json:"host,omitempty"`
	Schema       string `json:"schema,omitempty"`
	Login        string `json:"login,omitempty"`
	Password     string `json:"password,omitempty"`
	Port         int    `json:"port,omitempty"`
	Extra        string `json:"extra,omitempty"`
}

func (s *AirflowService) GetDAGs() ([]AirflowDAG, error) {
	log.Printf("[AIRFLOW] Getting list of DAGs")

	url := fmt.Sprintf("%s/api/v1/dags", s.Cfg.Airflow.BaseURL)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("[AIRFLOW] ERROR: Failed to create request: %v", err)
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.SetBasicAuth(s.Cfg.Airflow.Username, s.Cfg.Airflow.Password)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: time.Duration(s.Cfg.Airflow.Timeout) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[AIRFLOW] ERROR: Failed to execute request: %v", err)
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("[AIRFLOW] Response status: %d", resp.StatusCode)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("[AIRFLOW] ERROR: Unexpected status code %d: %s", resp.StatusCode, string(body))
		return nil, fmt.Errorf("airflow API returned status %d", resp.StatusCode)
	}

	var response struct {
		DAGs []AirflowDAG `json:"dags"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		log.Printf("[AIRFLOW] ERROR: Failed to decode response: %v", err)
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	log.Printf("[AIRFLOW] Successfully retrieved %d DAGs", len(response.DAGs))
	return response.DAGs, nil
}

func (s *AirflowService) GetDAG(dagID string) (*AirflowDAG, error) {
	log.Printf("[AIRFLOW] Getting DAG: %s", dagID)

	url := fmt.Sprintf("%s/api/v1/dags/%s", s.Cfg.Airflow.BaseURL, dagID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("[AIRFLOW] ERROR: Failed to create request: %v", err)
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.SetBasicAuth(s.Cfg.Airflow.Username, s.Cfg.Airflow.Password)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: time.Duration(s.Cfg.Airflow.Timeout) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[AIRFLOW] ERROR: Failed to execute request: %v", err)
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("[AIRFLOW] Response status: %d", resp.StatusCode)

	if resp.StatusCode == http.StatusNotFound {
		log.Printf("[AIRFLOW] DAG not found: %s", dagID)
		return nil, fmt.Errorf("DAG not found: %s", dagID)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("[AIRFLOW] ERROR: Unexpected status code %d: %s", resp.StatusCode, string(body))
		return nil, fmt.Errorf("airflow API returned status %d", resp.StatusCode)
	}

	var dag AirflowDAG
	if err := json.NewDecoder(resp.Body).Decode(&dag); err != nil {
		log.Printf("[AIRFLOW] ERROR: Failed to decode response: %v", err)
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	log.Printf("[AIRFLOW] Successfully retrieved DAG: %s", dagID)
	return &dag, nil
}

func (s *AirflowService) TriggerDAG(dagID string, conf map[string]interface{}) (*AirflowDAGRun, error) {
	log.Printf("[AIRFLOW] Triggering DAG: %s", dagID)

	url := fmt.Sprintf("%s/api/v1/dags/%s/dagRuns", s.Cfg.Airflow.BaseURL, dagID)

	payload := map[string]interface{}{
		"dag_run_id": fmt.Sprintf("manual_%d", time.Now().Unix()),
		"conf":       conf,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[AIRFLOW] ERROR: Failed to marshal payload: %v", err)
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	log.Printf("[AIRFLOW] Trigger payload: %s", string(payloadBytes))

	req, err := http.NewRequest("POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		log.Printf("[AIRFLOW] ERROR: Failed to create request: %v", err)
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.SetBasicAuth(s.Cfg.Airflow.Username, s.Cfg.Airflow.Password)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: time.Duration(s.Cfg.Airflow.Timeout) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[AIRFLOW] ERROR: Failed to execute request: %v", err)
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("[AIRFLOW] Response status: %d", resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	log.Printf("[AIRFLOW] Response body: %s", string(body))

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		log.Printf("[AIRFLOW] ERROR: Unexpected status code %d: %s", resp.StatusCode, string(body))
		return nil, fmt.Errorf("airflow API returned status %d: %s", resp.StatusCode, string(body))
	}

	var dagRun AirflowDAGRun
	if err := json.Unmarshal(body, &dagRun); err != nil {
		log.Printf("[AIRFLOW] ERROR: Failed to decode response: %v", err)
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	log.Printf("[AIRFLOW] Successfully triggered DAG: %s, run ID: %s", dagID, dagRun.DagRunID)
	return &dagRun, nil
}

func (s *AirflowService) GetDAGRuns(dagID string) ([]AirflowDAGRun, error) {
	log.Printf("[AIRFLOW] Getting DAG runs for: %s", dagID)

	url := fmt.Sprintf("%s/api/v1/dags/%s/dagRuns", s.Cfg.Airflow.BaseURL, dagID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("[AIRFLOW] ERROR: Failed to create request: %v", err)
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.SetBasicAuth(s.Cfg.Airflow.Username, s.Cfg.Airflow.Password)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: time.Duration(s.Cfg.Airflow.Timeout) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[AIRFLOW] ERROR: Failed to execute request: %v", err)
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("[AIRFLOW] Response status: %d", resp.StatusCode)

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("[AIRFLOW] ERROR: Unexpected status code %d: %s", resp.StatusCode, string(body))
		return nil, fmt.Errorf("airflow API returned status %d", resp.StatusCode)
	}

	var response struct {
		DAGRuns []AirflowDAGRun `json:"dag_runs"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		log.Printf("[AIRFLOW] ERROR: Failed to decode response: %v", err)
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	log.Printf("[AIRFLOW] Successfully retrieved %d DAG runs", len(response.DAGRuns))
	return response.DAGRuns, nil
}

func (s *AirflowService) SetupConnections() error {
	log.Printf("[AIRFLOW] Setting up database connections")

	connections := []AirflowConnection{
		{
			ConnectionID: "pg_target",
			ConnType:     "postgres",
			Host:         "postgres-data",
			Schema:       "datawarehouse",
			Login:        "datauser",
			Password:     "datapass",
			Port:         5432,
		},
		{
			ConnectionID: "ch_target",
			ConnType:     "http",
			Host:         "clickhouse",
			Schema:       "analytics",
			Login:        "clickuser",
			Password:     "clickpass",
			Port:         8123,
			Extra:        `{"endpoint": "/", "headers": {"Content-Type": "application/json"}}`,
		},
	}

	for _, conn := range connections {
		if err := s.createConnection(conn); err != nil {
			log.Printf("[AIRFLOW] WARNING: Failed to create connection %s: %v", conn.ConnectionID, err)
			// Не прерываем процесс, если не удалось создать соединение
		} else {
			log.Printf("[AIRFLOW] Successfully created connection: %s", conn.ConnectionID)
		}
	}

	return nil
}

func (s *AirflowService) createConnection(conn AirflowConnection) error {
	url := fmt.Sprintf("%s/api/v1/connections", s.Cfg.Airflow.BaseURL)

	payloadBytes, err := json.Marshal(conn)
	if err != nil {
		return fmt.Errorf("failed to marshal connection: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.SetBasicAuth(s.Cfg.Airflow.Username, s.Cfg.Airflow.Password)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: time.Duration(s.Cfg.Airflow.Timeout) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		// Connection already exists, try to update it
		return s.updateConnection(conn)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("airflow API returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (s *AirflowService) updateConnection(conn AirflowConnection) error {
	url := fmt.Sprintf("%s/api/v1/connections/%s", s.Cfg.Airflow.BaseURL, conn.ConnectionID)

	payloadBytes, err := json.Marshal(conn)
	if err != nil {
		return fmt.Errorf("failed to marshal connection: %w", err)
	}

	req, err := http.NewRequest("PATCH", url, bytes.NewReader(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.SetBasicAuth(s.Cfg.Airflow.Username, s.Cfg.Airflow.Password)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: time.Duration(s.Cfg.Airflow.Timeout) * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("airflow API returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
