package main

import (
	"log"
	"net/http"
	"strings"
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

	// Logging middleware
	r.Use(loggingMiddleware())

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
	analyzeSvc := &usecase.AnalyzeService{Hub: hub, Cfg: cfg}
	pipelineSvc := &usecase.PipelineService{Hub: hub, Cfg: cfg}
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
	log.Printf("server listening on %s (ml: %s)", cfg.Server.Addr, cfg.MLBaseURL)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func corsMiddleware(allowedOrigins []string) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin != "" {
				allowed, allowAny := isAllowedOrigin(origin, allowedOrigins)
				if allowAny {
					w.Header().Set("Access-Control-Allow-Origin", "*")
					w.Header().Del("Access-Control-Allow-Credentials")
				} else if allowed {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Access-Control-Allow-Credentials", "true")
				}
				if allowed || allowAny {
					w.Header().Set("Vary", "Origin")
					w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
					w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization,X-Requested-With")
					w.Header().Set("Access-Control-Max-Age", "600")
				}
			}
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func isAllowedOrigin(origin string, allowed []string) (bool, bool) {
	// allowAny=true when ALLOWED_ORIGINS contains "*"
	normalized := strings.TrimRight(origin, "/")
	for _, o := range allowed {
		if o == "*" {
			return true, true
		}
		if strings.TrimRight(o, "/") == normalized {
			return true, false
		}
	}
	return false, false
}

func loggingMiddleware() mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Check if this is a WebSocket upgrade request
			isWebSocket := r.Header.Get("Upgrade") == "websocket"

			// Log incoming request
			log.Printf("[SERVER] [%s] %s %s from %s%s",
				start.Format("15:04:05.000"),
				r.Method,
				r.URL.Path,
				r.RemoteAddr,
				func() string {
					if isWebSocket {
						return " (WebSocket)"
					}
					return ""
				}())

			// Log headers for non-WebSocket requests
			if !isWebSocket {
				log.Printf("[SERVER] User-Agent: %s", r.Header.Get("User-Agent"))
				log.Printf("[SERVER] Content-Type: %s", r.Header.Get("Content-Type"))
				log.Printf("[SERVER] Content-Length: %s", r.Header.Get("Content-Length"))
			} else {
				log.Printf("[SERVER] WebSocket headers - Connection: %s, Sec-WebSocket-Key: %s",
					r.Header.Get("Connection"),
					r.Header.Get("Sec-WebSocket-Key"))
			}

			// Create a response writer wrapper to capture status code
			wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

			// Process request
			next.ServeHTTP(wrapped, r)

			// Log response (WebSocket connections might not have meaningful status codes after upgrade)
			duration := time.Since(start)
			if !isWebSocket || wrapped.statusCode != http.StatusSwitchingProtocols {
				log.Printf("[SERVER] [%s] %s %s -> %d (%v)",
					time.Now().Format("15:04:05.000"),
					r.Method,
					r.URL.Path,
					wrapped.statusCode,
					duration)
			} else {
				log.Printf("[SERVER] [%s] %s %s -> WebSocket upgraded (%v)",
					time.Now().Format("15:04:05.000"),
					r.Method,
					r.URL.Path,
					duration)
			}
		})
	}
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
