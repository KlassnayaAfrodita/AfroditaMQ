package broker

import (
	"testing"
	"time"
)

func BenchmarkPublish(b *testing.B) {
	broker := NewBroker()
	topic := "benchmark-topic"
	err := broker.CreateTopic(topic)
	if err != nil {
		b.Fatalf("failed to create topic: %v", err)
	}

	message := Message{
		Content:    "Benchmark message",
		Priority:   1,
		Expiration: time.Now().Add(10 * time.Second), // Длинный TTL
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := broker.Publish(topic, message); err != nil {
			b.Fatalf("failed to publish message: %v", err)
		}
	}
}

func BenchmarkReceive(b *testing.B) {
	broker := NewBroker()
	topic := "benchmark-topic"
	err := broker.CreateTopic(topic)
	if err != nil {
		b.Fatalf("failed to create topic: %v", err)
	}

	clientID := "client1"
	err = broker.Subscribe(clientID, topic)
	if err != nil {
		b.Fatalf("failed to subscribe client: %v", err)
	}

	// Создаем сообщение с длительным TTL
	message := Message{
		Content:    "Benchmark message",
		Priority:   1,
		Expiration: time.Now().Add(10 * time.Second), // Длинный TTL
	}

	// Публикуем сообщение до начала тестирования
	if err := broker.Publish(topic, message); err != nil {
		b.Fatalf("failed to publish message: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Получаем сообщение
		receivedMsg, err := broker.ReceiveFromTopic(clientID)
		if err != nil {
			b.Fatalf("failed to receive message: %v", err)
		}

		// Подтверждаем сообщение
		if err := broker.AcknowledgeMessage(clientID); err != nil {
			b.Fatalf("failed to acknowledge message: %v", err)
		}

		// Повторно публикуем сообщение для теста
		err = broker.Publish(topic, Message{
			Content:    receivedMsg,                      // Используем полученное сообщение
			Priority:   1,                                // Устанавливаем приоритет
			Expiration: time.Now().Add(10 * time.Second), // Новый TTL
		})
		if err != nil {
			b.Fatalf("failed to republish message: %v", err)
		}
	}
}

func BenchmarkCleanUpExpiredMessages(b *testing.B) {
	broker := NewBroker()
	topic := "benchmark-topic"
	err := broker.CreateTopic(topic)
	if err != nil {
		b.Fatalf("failed to create topic: %v", err)
	}

	// Генерируем устаревшие сообщения
	const numMessages = 1000
	expiredTime := time.Now().Add(-1 * time.Second) // Все сообщения уже истекли

	for i := 0; i < numMessages; i++ {
		message := Message{
			Content:    "Expired message",
			Priority:   1,
			Expiration: expiredTime,
		}
		if err := broker.Publish(topic, message); err != nil {
			b.Fatalf("failed to publish message: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		broker.CleanUpExpiredMessages()
	}
}
