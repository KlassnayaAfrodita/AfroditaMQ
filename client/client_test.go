package client

import (
	"MessageBroker/broker"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Mock server для имитации ответа от сервера
func mockServer(responseCode int, responseBody string) *httptest.Server {
	handler := http.NewServeMux()

	handler.HandleFunc("/acknowledge", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
			return
		}

		var request map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		clientID, ok := request["client_id"].(string)
		if !ok || clientID != "test_client" {
			http.Error(w, "Invalid client_id", http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"success"}`))
	})

	// Обработчик для публикации сообщений
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

	// Обработчик для подписки на топик
	handler.HandleFunc("/subscribe", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
			return
		}

		// Проверяем, что тело запроса содержит правильный топик
		var request map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		topic, ok := request["topic"].(string)
		if !ok || topic == "" {
			http.Error(w, "Invalid topic", http.StatusBadRequest)
			return
		}

		// Симулируем успешную подписку
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"subscribed"}`))
	})

	// Обработчик для получения сообщений
	handler.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
			return
		}

		// Симулируем получение сообщения
		// Можно сделать проверку, что сообщение доступно для получения
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"message":"Breaking news"}`)) // Имитация сообщения
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
	publisher := NewClientPublisher("client1", server.URL)

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
	publisher := NewClientPublisher("client1", server.URL)

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
	publisher := NewClientPublisher("client1", server.URL)

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

// Тестируем публикацию сообщения с TTL
func TestPublishMessageWithTTL(t *testing.T) {
	// Создаем мок сервер
	server := mockServer(http.StatusOK, `{"status":"success"}`)
	defer server.Close()

	// Создаем Publisher
	publisher := NewClientPublisher("client1", server.URL)

	ttl := int64(2) // 2 секунды TTL
	topic := "news"
	message := "Breaking news"
	priority := 1

	// Публикуем сообщение
	err := publisher.Publish(topic, message, priority, ttl)
	assert.Nil(t, err, "Ошибка при публикации сообщения")
}

// Тестируем получение сообщения до истечения TTL
func TestReceiveMessageBeforeTTLExpires(t *testing.T) {
	// Создаем мок сервер
	server := mockServer(http.StatusOK, `{"status":"success"}`)
	defer server.Close()

	clientID := "client1"
	baseURL := server.URL

	subscriber := NewClientSubscriber(clientID, baseURL)

	ttl := int64(2) // 2 секунды TTL
	topic := "news"
	message := "Breaking news"

	// Подписываемся на топик
	err := subscriber.Subscribe(topic)
	assert.Nil(t, err, "Ошибка при подписке на топик")

	// Публикуем сообщение
	publisher := NewClientPublisher(clientID, baseURL)
	err = publisher.Publish(topic, message, 1, ttl)
	assert.Nil(t, err, "Ошибка при публикации сообщения")

	// Ожидаем, пока TTL не истечет
	time.Sleep(1 * time.Second)

	// Пытаемся получить сообщение
	respMessage, err := subscriber.ReceiveMessage() // Предполагаем, что есть метод ReceiveMessage
	assert.Nil(t, err, "Ошибка при получении сообщения")

	// Делаем парсинг полученного ответа
	var respData map[string]interface{}
	err = json.Unmarshal([]byte(respMessage), &respData)
	if err != nil {
		t.Fatalf("Ошибка при парсинге сообщения: %v", err)
	}

	// Проверяем, что в полученном сообщении содержится ожидаемое
	assert.Equal(t, message, respData["message"], "Сообщение не совпадает с ожидаемым")
}

// Тест на получение сообщения после истечения TTL
func TestReceiveMessageAfterTTLExpires(t *testing.T) {
	// Создаем брокер
	b := broker.NewBroker()
	defer b.Close()

	// Создаем топик
	topic := "news"
	err := b.CreateTopic(topic)
	assert.Nil(t, err, "Ошибка при создании топика")

	// Создаем сообщение с TTL 1 секунда
	message := broker.Message{
		Content:    "Breaking news",
		Priority:   1,
		Expiration: time.Now().Add(1 * time.Second), // TTL - 1 секунда
	}

	// Публикуем сообщение
	err = b.Publish(topic, message)
	assert.Nil(t, err, "Ошибка при публикации сообщения")

	// Подписываемся на топик
	clientID := "client1"
	err = b.Subscribe(clientID, topic)
	assert.Nil(t, err, "Ошибка при подписке на топик")

	// Ждём завершения времени TTL и удаления сообщения из брокера
	time.Sleep(2 * time.Second)

	// Повторно проверяем очистку истёкших сообщений
	time.Sleep(500 * time.Millisecond) // Даем немного времени на выполнение CleanUpExpiredMessages

	// Пытаемся получить сообщение после истечения TTL
	respMessage, err := b.ReceiveFromTopic(clientID)

	// Проверяем, что ошибка возникла, так как сообщение истекло
	assert.NotNil(t, err, "Ошибка не была получена после истечения TTL")
	assert.Equal(t, "", respMessage, "Сообщение не должно быть получено после истечения TTL")
}

func TestAcknowledgeMessage(t *testing.T) {
	server := mockServer(http.StatusOK, `{"status":"success"}`)
	defer server.Close()

	client := NewClientSubscriber("test_client", server.URL)

	err := client.AcknowledgeMessage()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
}

func TestReceiveMessage(t *testing.T) {
	server := mockServer(http.StatusOK, `{"message":"Breaking news"}`)
	defer server.Close()

	client := NewClientSubscriber("test_client", server.URL)

	// Получаем сообщение от сервера
	message, err := client.ReceiveMessage()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Парсим полученное сообщение (это JSON)
	var respData map[string]interface{}
	err = json.Unmarshal([]byte(message), &respData)
	if err != nil {
		t.Fatalf("Error parsing received message: %v", err)
	}

	// Проверяем, что значение поля "message" соответствует ожиданиям
	expectedMessage := "Breaking news"
	if respData["message"] != expectedMessage {
		t.Fatalf("Expected message '%s', got '%s'", expectedMessage, respData["message"])
	}
}
