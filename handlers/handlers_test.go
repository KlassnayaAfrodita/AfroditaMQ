package handlers

import (
	"MessageBroker/broker" // Убедитесь, что правильно импортирован брокер
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Тест для обработчика создания топика
func TestCreateTopicHandler(t *testing.T) {
	b := broker.NewBroker()
	handler := CreateTopicHandler(b)

	reqBody := `{"topic": "news"}`
	req := httptest.NewRequest(http.MethodPost, "/create-topic", bytes.NewReader([]byte(reqBody)))
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", w.Code)
	}

	if w.Body.String() != "Topic created successfully\n" {
		t.Errorf("Expected response body 'Topic created successfully', got %s", w.Body.String())
	}
}

// Тест для обработчика публикации сообщений
func TestPublishHandler(t *testing.T) {
	b := broker.NewBroker()
	b.CreateTopic("news")
	handler := PublishHandler(b)

	reqBody := `{"topic": "news", "message": "Breaking news", "priority": 5, "ttl": 60}`
	req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewReader([]byte(reqBody)))
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", w.Code)
	}

	if w.Body.String() != "Message published successfully\n" {
		t.Errorf("Expected response body 'Message published successfully', got %s", w.Body.String())
	}
}

// Тест для обработчика отписки от топика
func TestUnsubscribeHandler(t *testing.T) {
	b := broker.NewBroker()
	b.CreateTopic("news")
	b.Subscribe("client1", "news")
	handler := UnsubscribeHandler(b)

	reqBody := `{"client_id": "client1", "topic": "news"}`
	req := httptest.NewRequest(http.MethodPost, "/unsubscribe", bytes.NewReader([]byte(reqBody)))
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", w.Code)
	}

	if w.Body.String() != "Unsubscribed successfully\n" {
		t.Errorf("Expected response body 'Unsubscribed successfully', got %s", w.Body.String())
	}

	// Попробуем отписаться от несуществующего топика
	reqBody = `{"client_id": "client1", "topic": "sports"}`
	req = httptest.NewRequest(http.MethodPost, "/unsubscribe", bytes.NewReader([]byte(reqBody)))
	w = httptest.NewRecorder()

	handler(w, req)

	if w.Code == http.StatusOK {
		t.Errorf("Expected error, got status code %d", w.Code)
	}
}
