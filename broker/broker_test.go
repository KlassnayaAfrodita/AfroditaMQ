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

// Тест на пакетное получение сообщений
func TestReceiveBatchFromTopic(t *testing.T) {
	broker := NewBroker()
	defer broker.Close()

	// Создаем топик и подписываем клиента
	err := broker.CreateTopic("news")
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	broker.Subscribe("client1", "news")

	// Публикуем несколько сообщений
	broker.Publish("news", Message{Content: "Message 1", Expiration: time.Now().Add(1 * time.Minute)})
	broker.Publish("news", Message{Content: "Message 2", Expiration: time.Now().Add(1 * time.Minute)})
	broker.Publish("news", Message{Content: "Message 3", Expiration: time.Now().Add(1 * time.Minute)})

	// Получаем пакет из 2 сообщений
	batch, err := broker.ReceiveBatchFromTopic("client1", "news", 2)
	if err != nil {
		t.Fatalf("Failed to receive batch: %v", err)
	}

	// Проверяем количество полученных сообщений
	if len(batch) != 2 {
		t.Errorf("Expected 2 messages, got %d", len(batch))
	}

	// Проверяем содержимое сообщений
	if batch[0] != "Message 1" || batch[1] != "Message 2" {
		t.Errorf("Unexpected batch content: %v", batch)
	}

	// Получаем оставшееся сообщение
	batch, err = broker.ReceiveBatchFromTopic("client1", "news", 2)
	if err != nil {
		t.Fatalf("Failed to receive remaining message: %v", err)
	}
	if len(batch) != 1 || batch[0] != "Message 3" {
		t.Errorf("Expected 1 message 'Message 3', got %v", batch)
	}

	// Проверяем поведение при отсутствии сообщений
	batch, err = broker.ReceiveBatchFromTopic("client1", "news", 2)
	if err == nil || len(batch) != 0 {
		t.Errorf("Expected no messages, got %v", batch)
	}
}

func TestPublishBatch(t *testing.T) {
	broker := NewBroker()
	defer broker.Close()

	// Создаем топик
	err := broker.CreateTopic("test_topic")
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	// Пакет сообщений
	messages := []Message{
		{Content: "Message 1", Priority: 1, Expiration: time.Now().Add(10 * time.Second)},
		{Content: "Message 2", Priority: 2, Expiration: time.Now().Add(15 * time.Second)},
		{Content: "Message 3", Priority: 3, Expiration: time.Now().Add(5 * time.Second)},
	}

	// Публикуем пакет
	err = broker.PublishBatch("test_topic", messages)
	if err != nil {
		t.Fatalf("PublishBatch failed: %v", err)
	}

	// Проверяем, что сообщения добавлены
	broker.mu.RLock()
	defer broker.mu.RUnlock()
	if len(broker.topics["test_topic"]) != len(messages) {
		t.Fatalf("expected %d messages, got %d", len(messages), len(broker.topics["test_topic"]))
	}

	// Проверяем, что сообщения соответствуют
	for i, msg := range broker.topics["test_topic"] {
		if msg.Content != messages[i].Content {
			t.Errorf("expected message content %s, got %s", messages[i].Content, msg.Content)
		}
	}
}

func TestPublishBatchToNonexistentTopic(t *testing.T) {
	broker := NewBroker()
	defer broker.Close()

	// Пакет сообщений
	messages := []Message{
		{Content: "Message 1", Priority: 1, Expiration: time.Now().Add(10 * time.Second)},
	}

	// Публикуем пакет в несуществующий топик
	err := broker.PublishBatch("nonexistent_topic", messages)
	if err == nil {
		t.Fatal("expected error when publishing to nonexistent topic, got nil")
	}
}
