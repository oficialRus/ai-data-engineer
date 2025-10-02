package usecase

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
	"github.com/user/ai-data-engineer/backend/internal/config"
)

type DatabaseService struct {
	Cfg config.AppConfig
}

type DatabaseInfo struct {
	Type      string `json:"type"`
	Host      string `json:"host"`
	Database  string `json:"database"`
	Connected bool   `json:"connected"`
	Error     string `json:"error,omitempty"`
}

type TableInfo struct {
	Name        string `json:"name"`
	Schema      string `json:"schema"`
	RowCount    int64  `json:"row_count"`
	ColumnCount int    `json:"column_count"`
}

func (s *DatabaseService) TestConnections() []DatabaseInfo {
	log.Printf("[DATABASE] Testing database connections")

	var results []DatabaseInfo

	// Test PostgreSQL
	pgInfo := s.testPostgreSQL()
	results = append(results, pgInfo)

	// Test ClickHouse
	chInfo := s.testClickHouse()
	results = append(results, chInfo)

	log.Printf("[DATABASE] Connection test completed")
	return results
}

func (s *DatabaseService) testPostgreSQL() DatabaseInfo {
	log.Printf("[DATABASE] Testing PostgreSQL connection")

	info := DatabaseInfo{
		Type:     "postgresql",
		Host:     "localhost:5433",
		Database: "datawarehouse",
	}

	db, err := sql.Open("postgres", s.Cfg.Database.PostgreSQLDataDSN)
	if err != nil {
		log.Printf("[DATABASE] ERROR: Failed to open PostgreSQL connection: %v", err)
		info.Connected = false
		info.Error = err.Error()
		return info
	}
	defer db.Close()

	// Set connection timeout
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		log.Printf("[DATABASE] ERROR: PostgreSQL ping failed: %v", err)
		info.Connected = false
		info.Error = err.Error()
		return info
	}

	log.Printf("[DATABASE] PostgreSQL connection successful")
	info.Connected = true
	return info
}

func (s *DatabaseService) testClickHouse() DatabaseInfo {
	log.Printf("[DATABASE] Testing ClickHouse connection")

	info := DatabaseInfo{
		Type:     "clickhouse",
		Host:     "localhost:8123",
		Database: s.Cfg.Database.ClickHouseDB,
	}

	// Test ClickHouse via HTTP
	url := fmt.Sprintf("%s/ping", s.Cfg.Database.ClickHouseURL)

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		log.Printf("[DATABASE] ERROR: ClickHouse HTTP request failed: %v", err)
		info.Connected = false
		info.Error = err.Error()
		return info
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[DATABASE] ERROR: ClickHouse returned status %d", resp.StatusCode)
		info.Connected = false
		info.Error = fmt.Sprintf("HTTP status %d", resp.StatusCode)
		return info
	}

	log.Printf("[DATABASE] ClickHouse connection successful")
	info.Connected = true
	return info
}

func (s *DatabaseService) GetPostgreSQLTables() ([]TableInfo, error) {
	log.Printf("[DATABASE] Getting PostgreSQL tables")

	db, err := sql.Open("postgres", s.Cfg.Database.PostgreSQLDataDSN)
	if err != nil {
		log.Printf("[DATABASE] ERROR: Failed to open PostgreSQL connection: %v", err)
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
		SELECT 
			t.table_name,
			t.table_schema,
			COALESCE(s.n_tup_ins + s.n_tup_upd + s.n_tup_del, 0) as estimated_rows,
			COUNT(c.column_name) as column_count
		FROM information_schema.tables t
		LEFT JOIN pg_stat_user_tables s ON s.relname = t.table_name
		LEFT JOIN information_schema.columns c ON c.table_name = t.table_name AND c.table_schema = t.table_schema
		WHERE t.table_type = 'BASE TABLE' 
		AND t.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
		GROUP BY t.table_name, t.table_schema, s.n_tup_ins, s.n_tup_upd, s.n_tup_del
		ORDER BY t.table_schema, t.table_name
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Printf("[DATABASE] ERROR: Failed to query PostgreSQL tables: %v", err)
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var table TableInfo
		if err := rows.Scan(&table.Name, &table.Schema, &table.RowCount, &table.ColumnCount); err != nil {
			log.Printf("[DATABASE] ERROR: Failed to scan table row: %v", err)
			continue
		}
		tables = append(tables, table)
	}

	if err := rows.Err(); err != nil {
		log.Printf("[DATABASE] ERROR: Error iterating table rows: %v", err)
		return nil, fmt.Errorf("error reading tables: %w", err)
	}

	log.Printf("[DATABASE] Found %d PostgreSQL tables", len(tables))
	return tables, nil
}

func (s *DatabaseService) CreateSampleData() error {
	log.Printf("[DATABASE] Creating sample data in PostgreSQL")

	db, err := sql.Open("postgres", s.Cfg.Database.PostgreSQLDataDSN)
	if err != nil {
		log.Printf("[DATABASE] ERROR: Failed to open PostgreSQL connection: %v", err)
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create sample tables
	queries := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(100) UNIQUE NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			is_active BOOLEAN DEFAULT true
		)`,
		`CREATE TABLE IF NOT EXISTS orders (
			id SERIAL PRIMARY KEY,
			user_id INTEGER REFERENCES users(id),
			amount DECIMAL(10,2) NOT NULL,
			status VARCHAR(20) DEFAULT 'pending',
			created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
		)`,
		`INSERT INTO users (name, email) VALUES 
			('John Doe', 'john@example.com'),
			('Jane Smith', 'jane@example.com'),
			('Bob Johnson', 'bob@example.com')
		ON CONFLICT (email) DO NOTHING`,
		`INSERT INTO orders (user_id, amount, status) VALUES 
			(1, 99.99, 'completed'),
			(1, 149.50, 'pending'),
			(2, 75.00, 'completed'),
			(3, 200.00, 'shipped')
		ON CONFLICT DO NOTHING`,
	}

	for i, query := range queries {
		log.Printf("[DATABASE] Executing query %d/%d", i+1, len(queries))
		if _, err := db.ExecContext(ctx, query); err != nil {
			log.Printf("[DATABASE] ERROR: Failed to execute query %d: %v", i+1, err)
			return fmt.Errorf("failed to execute query %d: %w", i+1, err)
		}
	}

	log.Printf("[DATABASE] Sample data created successfully")
	return nil
}
