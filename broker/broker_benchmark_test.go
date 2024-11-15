package broker

import (
	"testing"
	"time"
)

func BenchmarkPublish(b *testing.B) {
	broker := NewBroker()
	topic := "benchmark-topic"
	_ = broker.CreateTopic(topic)

	message := Message{
		Content:    "Benchmark message",
		Priority:   1,
		Expiration: time.Now().Add(1 * time.Second), // TTL - 1 секунда
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = broker.Publish(topic, message)
	}
}

func BenchmarkReceive(b *testing.B) {
	broker := NewBroker()
	topic := "benchmark-topic"
	_ = broker.CreateTopic(topic)

	clientID := "client1"
	_ = broker.Subscribe(clientID, topic)

	message := Message{
		Content:    "Benchmark message",
		Priority:   1,
		Expiration: time.Now().Add(10 * time.Second), // Длинный TTL
	}
	_ = broker.Publish(topic, message)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = broker.ReceiveFromTopic(clientID)
	}
}

func BenchmarkCleanUpExpiredMessages(b *testing.B) {
	broker := NewBroker()
	topic := "benchmark-topic"
	_ = broker.CreateTopic(topic)

	// Генерируем устаревшие сообщения
	for i := 0; i < 1000; i++ {
		message := Message{
			Content:    "Expired message",
			Priority:   1,
			Expiration: time.Now().Add(-1 * time.Second), // Сообщение уже устарело
		}
		_ = broker.Publish(topic, message)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		broker.CleanUpExpiredMessages()
	}
}
