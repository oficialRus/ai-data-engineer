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

	// CORS middleware
	r.Use(corsMiddleware(cfg.Server.AllowedOrigins))

	// Preflight handler
	r.Methods(http.MethodOptions).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

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

func corsMiddleware(allowedOrigins []string) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin != "" && isAllowedOrigin(origin, allowedOrigins) {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Vary", "Origin")
				w.Header().Set("Access-Control-Allow-Credentials", "true")
				w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization,X-Requested-With")
				w.Header().Set("Access-Control-Max-Age", "600")
			}
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func isAllowedOrigin(origin string, allowed []string) bool {
	for _, o := range allowed {
		if o == origin {
			return true
		}
	}
	return false
}
