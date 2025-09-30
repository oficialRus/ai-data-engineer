package ws

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/websocket"
)

type SubscriptionKey string

type Hub struct {
	subscribers map[SubscriptionKey]map[*websocket.Conn]struct{}
	register    chan hubRegister
	unregister  chan hubUnregister
	broadcast   chan hubBroadcast
}

type hubRegister struct {
	key    SubscriptionKey
	client *websocket.Conn
}

type hubUnregister struct {
	key    SubscriptionKey
	client *websocket.Conn
}

type hubBroadcast struct {
	key   SubscriptionKey
	event any
}

func NewHub() *Hub {
	return &Hub{
		subscribers: make(map[SubscriptionKey]map[*websocket.Conn]struct{}),
		register:    make(chan hubRegister),
		unregister:  make(chan hubUnregister),
		broadcast:   make(chan hubBroadcast, 128),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case reg := <-h.register:
			if h.subscribers[reg.key] == nil {
				h.subscribers[reg.key] = make(map[*websocket.Conn]struct{})
			}
			h.subscribers[reg.key][reg.client] = struct{}{}
		case unreg := <-h.unregister:
			if set, ok := h.subscribers[unreg.key]; ok {
				delete(set, unreg.client)
				if len(set) == 0 {
					delete(h.subscribers, unreg.key)
				}
			}
		case msg := <-h.broadcast:
			if set, ok := h.subscribers[msg.key]; ok {
				for c := range set {
					_ = c.WriteJSON(msg.event)
				}
			}
		}
	}
}

func Key(topic, id string) SubscriptionKey { return SubscriptionKey(topic + ":" + id) }

var upgrader = websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}

func (h *Hub) HandleWS(w http.ResponseWriter, r *http.Request, onCommand func(cmdType string, data json.RawMessage, conn *websocket.Conn)) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	for {
		var cmd struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := conn.ReadJSON(&cmd); err != nil {
			return
		}
		onCommand(cmd.Type, cmd.Data, conn)
	}
}

func (h *Hub) Subscribe(topic, id string, conn *websocket.Conn) {
	h.register <- hubRegister{key: Key(topic, id), client: conn}
}

func (h *Hub) UnsubscribeAll(conn *websocket.Conn, keys []SubscriptionKey) {
	for _, k := range keys {
		h.unregister <- hubUnregister{key: k, client: conn}
	}
}

func (h *Hub) Broadcast(topic, id string, event any) {
	h.broadcast <- hubBroadcast{key: Key(topic, id), event: event}
}
