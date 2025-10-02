package wsiface

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	infra "github.com/user/ai-data-engineer/backend/internal/infra/ws"
)

type ServerEvents struct {
	Queued   func(id, scope string) any
	Started  func(id, scope string) any
	Progress func(id, scope string, percent int, stage, message *string) any
	Log      func(id, scope, level, line string) any
	Done     func(scope, id string, payload any) any
	Error    func(id string, scope *string, reason string) any
	Resumed  func(pipelineId string) any
}

type WSHandlers struct {
	Hub *infra.Hub
}

type subscribePayload struct {
	Topic string `json:"topic"`
	ID    string `json:"id"`
}

type resumePayload struct {
	PipelineID string `json:"pipelineId"`
}

func (h *WSHandlers) HandleWS(w http.ResponseWriter, r *http.Request) {
	log.Printf("[WS_HANDLER] HandleWS called from %s", r.RemoteAddr)

	h.Hub.HandleWS(w, r, func(cmdType string, data json.RawMessage, conn *websocket.Conn) {
		log.Printf("[WS_HANDLER] Processing command: %s", cmdType)

		switch cmdType {
		case "subscribe":
			var p subscribePayload
			if err := json.Unmarshal(data, &p); err != nil {
				log.Printf("[WS_HANDLER] ERROR: Failed to unmarshal subscribe payload: %v", err)
				_ = conn.WriteJSON(map[string]any{"type": "error", "data": map[string]any{"reason": "invalid subscribe payload"}})
				return
			}
			log.Printf("[WS_HANDLER] Subscribing to topic: %s, id: %s", p.Topic, p.ID)
			log.Printf("[WS_HANDLER] Subscription key will be: %s", p.Topic+":"+p.ID)
			h.Hub.Subscribe(p.Topic, p.ID, conn)

			confirmMsg := map[string]any{"type": "subscribed", "data": map[string]any{"topic": p.Topic, "id": p.ID}}
			if err := conn.WriteJSON(confirmMsg); err != nil {
				log.Printf("[WS_HANDLER] Failed to send subscription confirmation: %v", err)
			} else {
				log.Printf("[WS_HANDLER] Sent subscription confirmation for topic: %s, id: %s", p.Topic, p.ID)
			}

		case "resume":
			var p resumePayload
			if err := json.Unmarshal(data, &p); err != nil {
				log.Printf("[WS_HANDLER] ERROR: Failed to unmarshal resume payload: %v", err)
				_ = conn.WriteJSON(map[string]any{"type": "error", "data": map[string]any{"reason": "invalid resume payload"}})
				return
			}
			log.Printf("[WS_HANDLER] Resuming pipeline: %s", p.PipelineID)
			_ = conn.WriteJSON(map[string]any{"type": "resumed", "data": map[string]any{"pipelineId": p.PipelineID}})

		default:
			log.Printf("[WS_HANDLER] ERROR: Unknown command type: %s", cmdType)
			_ = conn.WriteJSON(map[string]any{"type": "error", "data": map[string]any{"reason": "unknown command"}})
		}
	})
}
