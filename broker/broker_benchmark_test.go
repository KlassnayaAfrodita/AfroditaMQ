package broker

import (
	"strconv"
	"testing"
	"time"
)

// BenchmarkPublish измеряет производительность метода Publish
func BenchmarkPublish(b *testing.B) {
	broker := NewBroker()
	broker.CreateTopic("test_topic")

	message := Message{
		Content:    "test message",
		Priority:   1,
		Expiration: time.Now().Add(10 * time.Second),
		Topic:      "test_topic",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = broker.Publish("test_topic", message)
	}
}

// BenchmarkSubscribe измеряет производительность метода Subscribe
func BenchmarkSubscribe(b *testing.B) {
	broker := NewBroker()
	broker.CreateTopic("test_topic")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clientID := "client_" + strconv.Itoa(i)
		_ = broker.Subscribe(clientID, "test_topic")
	}
}

// BenchmarkReceiveFromTopic измеряет производительность метода ReceiveFromTopic
func BenchmarkReceiveFromTopic(b *testing.B) {
	broker := NewBroker()
	broker.CreateTopic("test_topic")
	clientID := "test_client"
	_ = broker.Subscribe(clientID, "test_topic")

	message := Message{
		Content:    "test message",
		Priority:   1,
		Expiration: time.Now().Add(10 * time.Second),
		Topic:      "test_topic",
	}
	for i := 0; i < 1000; i++ { // Наполняем топик сообщениями
		_ = broker.Publish("test_topic", message)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = broker.ReceiveFromTopic(clientID)
	}
}

// BenchmarkAcknowledgeMessage измеряет производительность метода AcknowledgeMessage
func BenchmarkAcknowledgeMessage(b *testing.B) {
	broker := NewBroker()
	broker.CreateTopic("test_topic")
	clientID := "test_client"
	_ = broker.Subscribe(clientID, "test_topic")

	message := Message{
		Content:    "test message",
		Priority:   1,
		Expiration: time.Now().Add(10 * time.Second),
		Topic:      "test_topic",
	}
	_ = broker.Publish("test_topic", message)
	_, _ = broker.ReceiveFromTopic(clientID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = broker.AcknowledgeMessage(clientID)
	}
}

// BenchmarkCleanUpExpiredMessages измеряет производительность метода CleanUpExpiredMessages
func BenchmarkCleanUpExpiredMessages(b *testing.B) {
	broker := NewBroker()
	broker.CreateTopic("test_topic")

	for i := 0; i < 1000; i++ {
		message := Message{
			Content:    "test message",
			Priority:   1,
			Expiration: time.Now().Add(-1 * time.Second), // Истёкшие сообщения
			Topic:      "test_topic",
		}
		_ = broker.Publish("test_topic", message)
	}

	time.Sleep(100 * time.Millisecond) // Даём немного времени на запуск очистки
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		broker.CleanUpExpiredMessages()
	}
}
