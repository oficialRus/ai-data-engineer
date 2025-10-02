package config

import (
	"log"
	"os"
	"strconv"
	"strings"
)

type ServerConfig struct {
	Addr           string
	AllowedOrigins []string
}

type LimitsConfig struct {
	MaxFileSizeBytes int64
}

type AppConfig struct {
	Server         ServerConfig
	Limits         LimitsConfig
	MLBaseURL      string
	MLAnalyzePath  string
	MLPipelinePath string
	MLTimeoutSec   int
}

func Load() AppConfig {
	log.Printf("[CONFIG] Loading application configuration...")

	cfg := AppConfig{
		Server:         ServerConfig{Addr: getEnv("SERVER_ADDR", ":8081"), AllowedOrigins: parseCSV(getEnv("ALLOWED_ORIGINS", "http://localhost:3000,http://45.150.9.52:3000"))},
		Limits:         LimitsConfig{MaxFileSizeBytes: parseSize(getEnv("MAX_FILE_SIZE", "10MB"))},
		MLBaseURL:      getEnv("ML_BASE_URL", "http://localhost:8000"),
		MLAnalyzePath:  getEnv("ML_ANALYZE_PATH", "/analyze"),
		MLPipelinePath: getEnv("ML_PIPELINE_PATH", "/pipelines"),
		MLTimeoutSec:   int(parseSize(getEnv("ML_TIMEOUT_SEC", "30"))),
	}

	log.Printf("[CONFIG] Server address: %s", cfg.Server.Addr)
	log.Printf("[CONFIG] Allowed origins: %v", cfg.Server.AllowedOrigins)
	log.Printf("[CONFIG] Max file size: %d bytes (%.2f MB)", cfg.Limits.MaxFileSizeBytes, float64(cfg.Limits.MaxFileSizeBytes)/(1024*1024))
	log.Printf("[CONFIG] ML base URL: %s", cfg.MLBaseURL)
	log.Printf("[CONFIG] ML analyze path: %s", cfg.MLAnalyzePath)
	log.Printf("[CONFIG] ML pipeline path: %s", cfg.MLPipelinePath)
	log.Printf("[CONFIG] ML timeout: %d seconds", cfg.MLTimeoutSec)
	log.Printf("[CONFIG] Configuration loaded successfully")

	return cfg
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func parseSize(s string) int64 {
	s = strings.TrimSpace(strings.ToUpper(s))
	if strings.HasSuffix(s, "MB") {
		n, err := strconv.ParseInt(strings.TrimSuffix(s, "MB"), 10, 64)
		if err != nil {
			log.Printf("[CONFIG] WARNING: Failed to parse MB size '%s', using default 10MB: %v", s, err)
			return 10 * 1024 * 1024
		}
		return n * 1024 * 1024
	}
	if strings.HasSuffix(s, "KB") {
		n, err := strconv.ParseInt(strings.TrimSuffix(s, "KB"), 10, 64)
		if err != nil {
			log.Printf("[CONFIG] WARNING: Failed to parse KB size '%s', using default 10MB: %v", s, err)
			return 10 * 1024 * 1024
		}
		return n * 1024
	}
	if strings.HasSuffix(s, "B") {
		n, err := strconv.ParseInt(strings.TrimSuffix(s, "B"), 10, 64)
		if err != nil {
			log.Printf("[CONFIG] WARNING: Failed to parse B size '%s', using default 10MB: %v", s, err)
			return 10 * 1024 * 1024
		}
		return n
	}
	// plain number means bytes
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		log.Printf("[CONFIG] WARNING: Failed to parse size '%s', using default 10MB: %v", s, err)
		return 10 * 1024 * 1024
	}
	return n
}

func parseCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
