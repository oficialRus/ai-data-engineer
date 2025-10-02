package main

import (
	"fmt"
	"log"
	"os"

	"github.com/user/ai-data-engineer/backend/internal/config"
	"github.com/user/ai-data-engineer/backend/internal/domain"
	"github.com/user/ai-data-engineer/backend/internal/usecase"
)

func main() {
	log.Println("=== Тестирование генерации DAG ===")

	// Загружаем конфигурацию
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Ошибка загрузки конфигурации: %v", err)
	}

	// Инициализируем сервис генерации DAG
	dagSvc := usecase.NewDAGGeneratorService(cfg)

	// Создаем тестовый запрос пайплайна
	testPipelineReq := domain.CreatePipelineRequest{
		SourceType: "csv",
		Source: map[string]interface{}{
			"pathOrUrl": "/data/test.csv",
			"delimiter": ",",
			"hasHeader": true,
			"encoding":  "utf-8",
		},
		Target: func(s string) *string { return &s }("postgresql"),
		DDL: map[string]interface{}{
			"postgresql": `
				CREATE TABLE IF NOT EXISTS test_imported_data (
					id SERIAL PRIMARY KEY,
					name VARCHAR(255),
					value NUMERIC(10,2),
					created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				);
			`,
		},
		Schedule: domain.ScheduleConfig{
			Cron:            "@daily",
			IncrementalMode: "full_refresh",
		},
	}

	// Генерируем ID пайплайна
	pipelineID := "test-dag-integration-" + fmt.Sprintf("%d", os.Getpid())

	log.Printf("Создан тестовый пайплайн с ID: %s", pipelineID)
	log.Printf("Источник: %s", testPipelineReq.SourceType)
	log.Printf("Цель: %s", *testPipelineReq.Target)
	log.Printf("Расписание: %s", testPipelineReq.Schedule.Cron)

	// Генерируем DAG файл
	log.Println("Генерация DAG файла...")
	if err := dagSvc.GenerateDAG(testPipelineReq, pipelineID); err != nil {
		log.Fatalf("Ошибка генерации DAG: %v", err)
	}

	log.Println("=== DAG файл создан successfully! ===")

	// Проверяем, что файл действительно создался
	dagFilePath := fmt.Sprintf("ops/airflow/dags/pipe_%s_v1.py", pipelineID)
	if _, err := os.Stat(dagFilePath); err == nil {
		log.Printf("✅ DAG файл найден: %s", dagFilePath)

		// Показываем содержимое файла (первые 20 строк)
		content, err := os.ReadFile(dagFilePath)
		if err == nil {
			log.Println("📄 Первые строки сгенерированного DAG:")
			lines := fmt.Sprintf("%s", content)[:min(500, len(content))]
			fmt.Println(lines)
		}
	} else {
		log.Printf("❌ DAG файл НЕ найден: %s", dagFilePath)
		log.Printf("Ошибка: %v", err)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
