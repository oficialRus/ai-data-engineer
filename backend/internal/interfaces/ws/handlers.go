package wsiface

import (
	"encoding/json"
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
	h.Hub.HandleWS(w, r, func(cmdType string, data json.RawMessage, conn *websocket.Conn) {
		switch cmdType {
		case "subscribe":
			var p subscribePayload
			if err := json.Unmarshal(data, &p); err == nil {
				h.Hub.Subscribe(p.Topic, p.ID, conn)
			}
		case "resume":
			var p resumePayload
			if err := json.Unmarshal(data, &p); err == nil {
				_ = conn.WriteJSON(map[string]any{"type": "resumed", "data": map[string]any{"pipelineId": p.PipelineID}})
			}
		default:
			_ = conn.WriteJSON(map[string]any{"type": "error", "data": map[string]any{"reason": "unknown command"}})
		}
	})
}
