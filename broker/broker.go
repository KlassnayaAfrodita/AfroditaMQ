package broker

import (
	"fmt"
	"sort"
	"time"
)

type Message struct {
	Content    string
	Priority   int
	Expiration time.Time
}

type Broker interface {
	Publish(topic string, message Message) error
	Subscribe(clientID string, topic string) error
	Unsubscribe(clientID string, topic string) error
	CreateTopic(topic string) error
	DeleteTopic(topic string) error
	ReceiveFromTopic(clientID string) (string, error)
	CleanUpExpiredMessages()
}

type SimpleBroker struct {
	topics  map[string][]Message
	clients map[string][]string
}

func NewBroker() *SimpleBroker {
	return &SimpleBroker{
		topics:  make(map[string][]Message),
		clients: make(map[string][]string),
	}
}

func (b *SimpleBroker) Publish(topic string, message Message) error {
	b.CleanUpExpiredMessages()

	if _, exists := b.topics[topic]; !exists {
		return fmt.Errorf("topic %s does not exist", topic)
	}

	// Добавляем сообщение в очередь для топика
	b.topics[topic] = append(b.topics[topic], message)

	// Сортируем сообщения по приоритету
	sort.SliceStable(b.topics[topic], func(i, j int) bool {
		return b.topics[topic][i].Priority > b.topics[topic][j].Priority
	})

	return nil
}

func (b *SimpleBroker) Subscribe(clientID string, topic string) error {
	b.clients[clientID] = append(b.clients[clientID], topic)
	return nil
}

func (b *SimpleBroker) Unsubscribe(clientID string, topic string) error {
	topics := b.clients[clientID]
	for i, t := range topics {
		if t == topic {
			b.clients[clientID] = append(topics[:i], topics[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("client %s not subscribed to topic %s", clientID, topic)
}

func (b *SimpleBroker) CreateTopic(topic string) error {
	if _, exists := b.topics[topic]; exists {
		return fmt.Errorf("topic %s already exists", topic)
	}
	b.topics[topic] = []Message{}
	return nil
}

func (b *SimpleBroker) DeleteTopic(topic string) error {
	delete(b.topics, topic)
	return nil
}

func (b *SimpleBroker) ReceiveFromTopic(clientID string) (string, error) {
	topics := b.clients[clientID]
	for _, topic := range topics {
		if messages, exists := b.topics[topic]; exists {
			for len(messages) > 0 {
				// Получаем первое сообщение из очереди
				message := messages[0]

				// Проверяем, истек ли TTL сообщения
				if message.Expiration.Before(time.Now()) {
					// Удаляем сообщение из очереди, если TTL истек
					messages = messages[1:]
					b.topics[topic] = messages // Обновляем очередь топика

					// Продолжаем проверку следующего сообщения в очереди
					continue
				}

				// Если TTL не истек, удаляем сообщение из очереди и возвращаем его
				b.topics[topic] = messages[1:]
				return message.Content, nil
			}
		}
	}
	return "", fmt.Errorf("no messages for client %s", clientID)
}

func (b *SimpleBroker) CleanUpExpiredMessages() {
	now := time.Now()
	for topic, messages := range b.topics {
		// Очищаем устаревшие сообщения
		filtered := []Message{}
		for _, msg := range messages {
			if msg.Expiration.After(now) {
				filtered = append(filtered, msg)
			}
		}
		b.topics[topic] = filtered
	}
}
