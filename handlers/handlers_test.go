package handlers

import (
	"MessageBroker/broker"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Тест для CreateTopicHandler
func TestCreateTopicHandler(t *testing.T) {
	b := broker.NewBroker()

	tests := []struct {
		name           string
		requestBody    CreateTopicRequest
		expectedStatus int
	}{
		{
			name: "Create new topic successfully",
			requestBody: CreateTopicRequest{
				Topic: "test-topic",
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "Try to create the same topic again",
			requestBody: CreateTopicRequest{
				Topic: "test-topic",
			},
			expectedStatus: http.StatusInternalServerError, // Тема уже существует
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqBody, _ := json.Marshal(tt.requestBody)
			req := httptest.NewRequest(http.MethodPost, "/create_topic", bytes.NewReader(reqBody))
			rec := httptest.NewRecorder()

			handler := CreateTopicHandler(b)
			handler.ServeHTTP(rec, req)

			if status := rec.Code; status != tt.expectedStatus {
				t.Errorf("expected status %v, got %v", tt.expectedStatus, status)
			}
		})
	}
}

// Тест для SubscribeHandler
func TestSubscribeHandler(t *testing.T) {
	b := broker.NewBroker()
	b.CreateTopic("test-topic")

	tests := []struct {
		name           string
		requestBody    SubscribeRequest
		expectedStatus int
	}{
		{
			name: "Subscribe successfully",
			requestBody: SubscribeRequest{
				ClientID: "client-1",
				Topic:    "test-topic",
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "Subscribe to a non-existent topic",
			requestBody: SubscribeRequest{
				ClientID: "client-2",
				Topic:    "non-existent-topic",
			},
			expectedStatus: http.StatusInternalServerError, // Ошибка, топик не существует
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqBody, _ := json.Marshal(tt.requestBody)
			req := httptest.NewRequest(http.MethodPost, "/subscribe", bytes.NewReader(reqBody))
			rec := httptest.NewRecorder()

			handler := SubscribeHandler(b)
			handler.ServeHTTP(rec, req)

			if status := rec.Code; status != tt.expectedStatus {
				t.Errorf("expected status %v, got %v", tt.expectedStatus, status)
			}
		})
	}
}

// Тест для PublishHandler
func TestPublishHandler(t *testing.T) {
	b := broker.NewBroker()
	b.CreateTopic("test-topic")

	tests := []struct {
		name           string
		requestBody    PublishRequest
		expectedStatus int
	}{
		{
			name: "Publish message successfully",
			requestBody: PublishRequest{
				Topic:    "test-topic",
				Message:  "Test message",
				Priority: 1,
				TTL:      60,
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "Publish message to non-existent topic",
			requestBody: PublishRequest{
				Topic:    "non-existent-topic",
				Message:  "Test message",
				Priority: 1,
				TTL:      60,
			},
			expectedStatus: http.StatusInternalServerError, // Ошибка, топик не существует
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqBody, _ := json.Marshal(tt.requestBody)
			req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader(reqBody))
			rec := httptest.NewRecorder()

			handler := PublishHandler(b)
			handler.ServeHTTP(rec, req)

			if status := rec.Code; status != tt.expectedStatus {
				t.Errorf("expected status %v, got %v", tt.expectedStatus, status)
			}
		})
	}
}

// Тест для ReceiveMessageHandler
func TestReceiveMessageHandler(t *testing.T) {
	b := broker.NewBroker()
	b.CreateTopic("test-topic")
	b.Publish("test-topic", broker.Message{
		Content:    "Test message",
		Priority:   1,
		Expiration: time.Now().Add(60 * time.Second),
	})

	// Подписываем клиента "client-1" на топик
	b.Subscribe("client-1", "test-topic")

	tests := []struct {
		name           string
		requestBody    ReceiveMessageRequest
		expectedStatus int
		expectedBody   string
	}{
		{
			name: "Receive message successfully",
			requestBody: ReceiveMessageRequest{
				ClientID: "client-1", // Клиент "client-1" подписан и получит сообщение
			},
			expectedStatus: http.StatusOK,
			expectedBody:   "Test message\n", // Сообщение, которое мы отправили
		},
		{
			name: "No message for client",
			requestBody: ReceiveMessageRequest{
				ClientID: "client-2", // Клиент "client-2" не подписан, сообщений нет
			},
			expectedStatus: http.StatusNotFound,
			expectedBody:   "no messages for client client-2\n", // Сообщение, которое возвращается теперь
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqBody, _ := json.Marshal(tt.requestBody)
			req := httptest.NewRequest(http.MethodPost, "/receive", bytes.NewReader(reqBody))
			rec := httptest.NewRecorder()

			handler := ReceiveMessageHandler(b)
			handler.ServeHTTP(rec, req)

			// Проверяем статус ответа
			if status := rec.Code; status != tt.expectedStatus {
				t.Errorf("expected status %v, got %v", tt.expectedStatus, status)
			}

			// Проверяем тело ответа
			if rec.Body.String() != tt.expectedBody {
				t.Errorf("expected body %v, got %v", tt.expectedBody, rec.Body.String())
			}
		})
	}
}

// Тест для AcknowledgeHandler
func TestAcknowledgeHandler(t *testing.T) {
	b := broker.NewBroker()
	b.CreateTopic("test-topic")
	b.Publish("test-topic", broker.Message{
		Content:    "Test message",
		Priority:   1,
		Expiration: time.Now().Add(60 * time.Second),
	})

	// Подписываем клиента
	b.Subscribe("client-1", "test-topic")

	// Получаем сообщение
	b.ReceiveFromTopic("client-1")

	tests := []struct {
		name           string
		requestBody    AcknowledgeRequest
		expectedStatus int
	}{
		{
			name: "Acknowledge message successfully",
			requestBody: AcknowledgeRequest{
				ClientID: "client-1",
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "Acknowledge without any unacknowledged message",
			requestBody: AcknowledgeRequest{
				ClientID: "client-2",
			},
			expectedStatus: http.StatusNotFound, // Нет необработанных сообщений
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reqBody, _ := json.Marshal(tt.requestBody)
			req := httptest.NewRequest(http.MethodPost, "/acknowledge", bytes.NewReader(reqBody))
			rec := httptest.NewRecorder()

			handler := AcknowledgeHandler(b)
			handler.ServeHTTP(rec, req)

			if status := rec.Code; status != tt.expectedStatus {
				t.Errorf("expected status %v, got %v", tt.expectedStatus, status)
			}
		})
	}
}
