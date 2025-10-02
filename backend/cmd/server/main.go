package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
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
	log.Printf("[MAIN] Starting AI Data Engineer server...")
	cfg := config.Load()
	log.Printf("[MAIN] Creating HTTP router...")
	r := mux.NewRouter()

	// Logging middleware
	log.Printf("[MAIN] Setting up logging middleware...")
	r.Use(loggingMiddleware())

	// CORS middleware
	log.Printf("[MAIN] Setting up CORS middleware with origins: %v", cfg.Server.AllowedOrigins)
	r.Use(corsMiddleware(cfg.Server.AllowedOrigins))

	// Preflight handler
	log.Printf("[MAIN] Setting up preflight OPTIONS handler...")
	r.Methods(http.MethodOptions).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	// WebSocket hub
	log.Printf("[MAIN] Initializing WebSocket hub...")
	hub := infraWS.NewHub()
	log.Printf("[MAIN] Starting WebSocket hub in background...")
	go hub.Run()

	// Usecases
	log.Printf("[MAIN] Initializing services...")
	analyzeSvc := &usecase.AnalyzeService{Hub: hub, Cfg: cfg}
	dagGenerator := &usecase.DAGGeneratorService{Cfg: cfg}
	pipelineSvc := &usecase.PipelineService{Hub: hub, Cfg: cfg, DAGGenerator: dagGenerator}
	previewSvc := &usecase.PreviewService{}
	log.Printf("[MAIN] Services initialized successfully")

	// Interfaces
	log.Printf("[MAIN] Initializing HTTP and WebSocket handlers...")
	httpHandlers := &httpiface.HTTPHandlers{AnalyzeSvc: analyzeSvc, PipelineSvc: pipelineSvc, PreviewSvc: previewSvc, Cfg: cfg}
	wsHandlers := &wsiface.WSHandlers{Hub: hub}

	log.Printf("[MAIN] Registering API routes...")
	r.HandleFunc("/api/preview", httpHandlers.Preview).Methods(http.MethodPost)
	r.HandleFunc("/api/analyze", httpHandlers.Analyze).Methods(http.MethodPost)
	r.HandleFunc("/api/pipelines", httpHandlers.CreatePipeline).Methods(http.MethodPost)
	r.HandleFunc("/ws", wsHandlers.HandleWS)
	log.Printf("[MAIN] API routes registered successfully")

	srv := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,  // Увеличено для больших файлов
		ReadTimeout:       60 * time.Second,  // Увеличено с 10 до 60 секунд
		WriteTimeout:      60 * time.Second,  // Увеличено с 10 до 60 секунд
		IdleTimeout:       120 * time.Second, // Увеличено с 60 до 120 секунд
	}
	log.Printf("server listening on %s (ml: %s)", cfg.Server.Addr, cfg.MLBaseURL)
	log.Printf("server configuration - ReadTimeout: %v, WriteTimeout: %v, MaxFileSize: %d bytes",
		60*time.Second, 60*time.Second, cfg.Limits.MaxFileSizeBytes)
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
	statusCode    int
	headerWritten bool
}

func (rw *responseWriter) WriteHeader(code int) {
	if !rw.headerWritten {
		rw.statusCode = code
		rw.headerWritten = true
		rw.ResponseWriter.WriteHeader(code)
	}
}

func (rw *responseWriter) Write(data []byte) (int, error) {
	if !rw.headerWritten {
		rw.WriteHeader(http.StatusOK)
	}
	return rw.ResponseWriter.Write(data)
}

// Hijack implements http.Hijacker interface for WebSocket support
func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := rw.ResponseWriter.(http.Hijacker); ok {
		return hijacker.Hijack()
	}
	return nil, nil, fmt.Errorf("responseWriter does not support hijacking")
}
