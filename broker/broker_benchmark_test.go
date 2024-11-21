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

// Бенчмарк для пакетного получения сообщений размером 10
func BenchmarkReceiveBatchFromTopicSize100(b *testing.B) {
	broker := NewBroker()
	defer broker.Close()

	broker.CreateTopic("news")
	broker.Subscribe("client1", "news")

	// Публикуем 100 сообщений
	for i := 0; i < 100; i++ {
		broker.Publish("news", Message{Content: "Message", Expiration: time.Now().Add(1 * time.Minute)})
	}

	for i := 0; i < b.N; i++ {
		broker.ReceiveBatchFromTopic("client1", "news", 10)
	}
}

// Бенчмарк для пакетного получения сообщений размером 1000
func BenchmarkReceiveBatchFromTopicSize1000(b *testing.B) {
	broker := NewBroker()
	defer broker.Close()

	broker.CreateTopic("news")
	broker.Subscribe("client1", "news")

	// Публикуем 100 сообщений
	for i := 0; i < 1000; i++ {
		broker.Publish("news", Message{Content: "Message", Expiration: time.Now().Add(1 * time.Minute)})
	}

	for i := 0; i < b.N; i++ {
		broker.ReceiveBatchFromTopic("client1", "news", 100)
	}
}

func BenchmarkPublishBatch(b *testing.B) {
	broker := NewBroker()
	defer broker.Close()

	// Создаем топик
	broker.CreateTopic("benchmark_topic")

	// Создаем пакет сообщений
	messages := make([]Message, 1000)
	for i := 0; i < 1000; i++ {
		messages[i] = Message{
			Content:    "Message",
			Priority:   i,
			Expiration: time.Now().Add(time.Duration(i) * time.Second),
		}
	}

	// Сброс счетчиков времени
	b.ResetTimer()

	// Выполняем публикацию в бенчмарке
	for i := 0; i < b.N; i++ {
		broker.PublishBatch("benchmark_topic", messages)
	}
}

func BenchmarkPublishBatchSmall(b *testing.B) {
	broker := NewBroker()
	defer broker.Close()

	// Создаем топик
	broker.CreateTopic("small_benchmark_topic")

	// Создаем небольшой пакет сообщений
	messages := make([]Message, 10)
	for i := 0; i < 10; i++ {
		messages[i] = Message{
			Content:    "Message",
			Priority:   i,
			Expiration: time.Now().Add(time.Duration(i) * time.Second),
		}
	}

	// Сброс счетчиков времени
	b.ResetTimer()

	// Выполняем публикацию в бенчмарке
	for i := 0; i < b.N; i++ {
		broker.PublishBatch("small_benchmark_topic", messages)
	}
}
