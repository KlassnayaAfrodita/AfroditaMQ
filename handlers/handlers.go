package handlers

import (
	"MessageBroker/broker"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// CreateTopicRequest представляет запрос на создание топика
type CreateTopicRequest struct {
	Topic string `json:"topic"`
}

// UnsubscribeRequest представляет запрос на отписку от топика
type UnsubscribeRequest struct {
	ClientID string `json:"client_id"`
	Topic    string `json:"topic"`
}

// SubscribeRequest представляет запрос на подписку на топик
type SubscribeRequest struct {
	ClientID string `json:"client_id"`
	Topic    string `json:"topic"`
}

// PublishRequest представляет запрос на публикацию сообщения
type PublishRequest struct {
	Topic    string `json:"topic"`
	Message  string `json:"message"`
	Priority int    `json:"priority"`
	TTL      int64  `json:"ttl"` // TTL в секундах
}

// PublishBatchRequest представляет запрос на пакетную публикацию сообщений
type PublishBatchRequest struct {
	Topic    string                   `json:"topic"`
	Messages []map[string]interface{} `json:"messages"`
}

// AcknowledgeRequest представляет запрос на подтверждение получения сообщения
type AcknowledgeRequest struct {
	ClientID string `json:"client_id"`
}

// ReceiveMessageRequest представляет запрос на получение сообщения
type ReceiveMessageRequest struct {
	ClientID string `json:"client_id"`
}

// SubscribeHandler подписывает клиента на топик
func SubscribeHandler(b *broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req SubscribeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Проверка существования топика
		if !b.TopicExists(req.Topic) {
			http.Error(w, "Topic does not exist", http.StatusInternalServerError) // Ошибка 500, если топик не существует
			return
		}

		// Осуществляем подписку
		if err := b.Subscribe(req.ClientID, req.Topic); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK) // Успешная подписка
	}
}

// UnsubscribeHandler отписывает клиента от топика
func UnsubscribeHandler(b *broker.Broker) http.HandlerFunc {
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

// CreateTopicHandler создает новый топик
func CreateTopicHandler(b *broker.Broker) http.HandlerFunc {
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

// PublishHandler публикует сообщение с приоритетом и TTL
func PublishHandler(b *broker.Broker) http.HandlerFunc {
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

// PublishBatchHandler публикует пакет сообщений на топик
func PublishBatchHandler(b *broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req PublishBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		var messages []broker.Message
		for _, msgData := range req.Messages {
			message := broker.Message{
				Content:    msgData["message"].(string),
				Priority:   int(msgData["priority"].(float64)),
				Expiration: time.Now().Add(time.Duration(msgData["ttl"].(float64)) * time.Second),
			}
			messages = append(messages, message)
		}

		if err := b.PublishBatch(req.Topic, messages); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "Batch of messages published successfully")
	}
}

// ReceiveMessageHandler получает сообщение для клиента
func ReceiveMessageHandler(b *broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req ReceiveMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		// Проверяем, есть ли у клиента сообщения
		message, err := b.ReceiveFromTopic(req.ClientID)
		if err != nil {
			// Если сообщений нет, отправляем понятное сообщение
			http.Error(w, fmt.Sprintf("no messages for client %s", req.ClientID), http.StatusNotFound)
			return
		}

		// Если сообщение найдено, отправляем его
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, message)
	}
}

// ReceiveBatchHandler получает пакет сообщений для клиента из топика
func ReceiveBatchHandler(b *broker.Broker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ClientID  string `json:"client_id"`
			Topic     string `json:"topic"`
			BatchSize int    `json:"batch_size"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		messages, err := b.ReceiveBatchFromTopic(req.ClientID, req.Topic, req.BatchSize)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(messages)
	}
}

// AcknowledgeHandler подтверждает получение сообщения
func AcknowledgeHandler(b *broker.Broker) http.HandlerFunc {
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
