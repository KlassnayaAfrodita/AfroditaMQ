package client

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Mock server для имитации ответа от сервера
func mockServer(responseCode int, responseBody string) *httptest.Server {
	handler := http.NewServeMux()
	handler.HandleFunc("/publish", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
			return
		}

		var request map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		w.WriteHeader(responseCode)
		w.Write([]byte(responseBody))
	})

	server := httptest.NewServer(handler)
	return server
}

// Тест на успешную публикацию сообщения
func TestPublishSuccess(t *testing.T) {
	// Создаём mock сервер
	server := mockServer(http.StatusOK, `{"status":"success"}`)
	defer server.Close()

	// Создаём Publisher
	publisher := NewPublisher("client1", server.URL)

	// Публикуем сообщение
	err := publisher.Publish("news", "Breaking news", 5, 60)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

// Тест на ошибку при публикации сообщения
func TestPublishError(t *testing.T) {
	// Создаём mock сервер, который возвращает ошибку
	server := mockServer(http.StatusInternalServerError, `{"status":"error"}`)
	defer server.Close()

	// Создаём Publisher
	publisher := NewPublisher("client1", server.URL)

	// Публикуем сообщение
	err := publisher.Publish("news", "Breaking news", 5, 60)
	if err == nil {
		t.Fatalf("Expected error, got nil")
	}

	// Проверяем сообщение об ошибке
	expectedError := "failed to publish message, status code: 500"
	if err.Error() != expectedError {
		t.Errorf("Expected error message '%s', got '%s'", expectedError, err.Error())
	}
}

// Тест на ошибку при некорректном запросе
func TestPublishInvalidRequest(t *testing.T) {
	// Создаём mock сервер, который будет генерировать ошибку из-за неверного запроса
	server := mockServer(http.StatusBadRequest, `{"status":"bad request"}`)
	defer server.Close()

	// Создаём Publisher
	publisher := NewPublisher("client1", server.URL)

	// Публикуем сообщение с неправильными параметрами
	// Например, пустое сообщение
	err := publisher.Publish("news", "", 5, 60)
	if err == nil {
		t.Fatalf("Expected error, got nil")
	}

	// Проверяем, что ошибка произошла из-за неверного ответа от сервера
	expectedError := "failed to publish message, status code: 400"
	if err.Error() != expectedError {
		t.Errorf("Expected error message '%s', got '%s'", expectedError, err.Error())
	}
}
