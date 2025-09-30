package main

import (
	"log"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/user/ai-data-engineer/backend/internal/config"
	infraWS "github.com/user/ai-data-engineer/backend/internal/infra/ws"
	httpiface "github.com/user/ai-data-engineer/backend/internal/interfaces/http"
	wsiface "github.com/user/ai-data-engineer/backend/internal/interfaces/ws"
	"github.com/user/ai-data-engineer/backend/internal/usecase"
)

func main() {
	cfg := config.Load()
	r := mux.NewRouter()

	// WebSocket hub
	hub := infraWS.NewHub()
	go hub.Run()

	// Usecases
	analyzeSvc := &usecase.AnalyzeService{Hub: hub}
	pipelineSvc := &usecase.PipelineService{Hub: hub}
	previewSvc := &usecase.PreviewService{}

	// Interfaces
	httpHandlers := &httpiface.HTTPHandlers{AnalyzeSvc: analyzeSvc, PipelineSvc: pipelineSvc, PreviewSvc: previewSvc, Cfg: cfg}
	wsHandlers := &wsiface.WSHandlers{Hub: hub}

	r.HandleFunc("/api/preview", httpHandlers.Preview).Methods(http.MethodPost)
	r.HandleFunc("/api/analyze", httpHandlers.Analyze).Methods(http.MethodPost)
	r.HandleFunc("/api/pipelines", httpHandlers.CreatePipeline).Methods(http.MethodPost)
	r.HandleFunc("/ws", wsHandlers.HandleWS)

	srv := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("server listening on %s", cfg.Server.Addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
