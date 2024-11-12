package handlers

import (
	"MessageBroker/broker"
	"encoding/json"
	"fmt"
	"net/http"
)

type PublishRequest struct {
	Topic   string `json:"topic"`
	Message string `json:"message"`
}

type SubscribeRequest struct {
	ClientID string `json:"client_id"`
	Topic    string `json:"topic"`
}

type ReceiveRequest struct {
	ClientID string `json:"client_id"`
}

// Обработчик публикации сообщения
func PublishHandler(b broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req PublishRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		if err := b.Publish(req.Topic, req.Message); err != nil {
			http.Error(w, "Failed to publish message", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "Message published successfully")
	}
}

// Обработчик подписки на топик
func SubscribeHandler(b broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SubscribeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		if err := b.Subscribe(req.ClientID, req.Topic); err != nil {
			http.Error(w, "Failed to subscribe", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "Subscribed successfully")
	}
}

// Обработчик получения сообщения
func ReceiveHandler(b broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req ReceiveRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		message, err := b.ReceiveFromTopic(req.ClientID)
		if err != nil {
			http.Error(w, "Failed to receive message", http.StatusNoContent)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, message)
	}
}
