package broker

import (
	"testing"
	"time"
)

func TestCreateTopic(t *testing.T) {
	b := NewBroker()

	err := b.CreateTopic("news")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	err = b.CreateTopic("news") // Повторное создание топика
	if err == nil {
		t.Errorf("Expected error for creating existing topic, got nil")
	}
}

func TestPublishAndReceiveMessage(t *testing.T) {
	// Инициализация брокера
	b := NewBroker()

	// Создание топика
	err := b.CreateTopic("news")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Подписка клиента на топик
	err = b.Subscribe("client1", "news")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Публикация сообщения
	message := Message{
		Content:    "Breaking news",
		Priority:   1,
		Expiration: time.Now().Add(time.Minute),
	}

	err = b.Publish("news", message)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Получение сообщения
	receivedMessage, err := b.ReceiveFromTopic("client1")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if receivedMessage != "Breaking news" {
		t.Errorf("Expected message 'Breaking news', got %v", receivedMessage)
	}
}

func TestSubscribeAndUnsubscribe(t *testing.T) {
	b := NewBroker()
	b.CreateTopic("news")

	b.Subscribe("client1", "news")
	err := b.Subscribe("client1", "news") // Повторная подписка
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	err = b.Unsubscribe("client1", "news")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	err = b.Unsubscribe("client1", "sports") // Отписка от несуществующего топика
	if err == nil {
		t.Errorf("Expected error for unsubscribing from non-existent topic, got nil")
	}
}
