package handlers

import (
	"MessageBroker/broker"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type CreateTopicRequest struct {
	Topic string `json:"topic"`
}

type UnsubscribeRequest struct {
	ClientID string `json:"client_id"`
	Topic    string `json:"topic"`
}

type SubscribeRequest struct {
	ClientID string `json:"client_id"`
	Topic    string `json:"topic"`
}

type PublishRequest struct {
	Topic    string `json:"topic"`
	Message  string `json:"message"`
	Priority int    `json:"priority"`
	TTL      int64  `json:"ttl"` // TTL в секундах
}

type AcknowledgeRequest struct {
	ClientID string `json:"client_id"`
}

type ReceiveMessageRequest struct {
	ClientID string `json:"client_id"`
}

// CreateTopicHandler создает новый топик
func CreateTopicHandler(b broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateTopicRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		if err := b.CreateTopic(req.Topic); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "Topic created successfully")
	}
}

// UnsubscribeHandler отписывает клиента от топика
func UnsubscribeHandler(b broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req UnsubscribeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		if err := b.Unsubscribe(req.ClientID, req.Topic); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "Unsubscribed successfully")
	}
}

// PublishHandler публикует сообщение с приоритетом и TTL
func PublishHandler(b broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req PublishRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		expiration := time.Now().Add(time.Duration(req.TTL) * time.Second)
		message := broker.Message{
			Content:    req.Message,
			Priority:   req.Priority,
			Expiration: expiration,
		}

		if err := b.Publish(req.Topic, message); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "Message published successfully")
	}
}

func SubscribeHandler(b broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SubscribeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		// Выполнение подписки
		if err := b.Subscribe(req.ClientID, req.Topic); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Ответ, если подписка прошла успешно
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "Subscribed successfully")
	}
}

// ReceiveMessageHandler получает сообщение для клиента
func ReceiveMessageHandler(b broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req ReceiveMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		message, err := b.ReceiveFromTopic(req.ClientID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, message)
	}
}

// AcknowledgeHandler подтверждает получение сообщения
func AcknowledgeHandler(b broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req AcknowledgeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		if err := b.AcknowledgeMessage(req.ClientID); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "Message acknowledged successfully")
	}
}
