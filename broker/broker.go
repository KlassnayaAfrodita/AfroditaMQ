package broker

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

// Message представляет сообщение
type Message struct {
	Content    string
	Priority   int
	Expiration time.Time
	Topic      string
	Index      int // для управления в heap
}

// MinHeap для управления сроками действия сообщений
type MinHeap []*Message

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i].Expiration.Before(h[j].Expiration) }
func (h MinHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].Index = i
	h[j].Index = j
}

func (h *MinHeap) Push(x interface{}) {
	item := x.(*Message)
	item.Index = len(*h)
	*h = append(*h, item)
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// Broker реализует интерфейс брокера
type Broker struct {
	topics         map[string][]*Message
	clients        map[string][]string
	messageHeap    MinHeap
	mu             sync.RWMutex
	unacknowledged map[string]*Message
	stopCleanup    chan struct{}
}

// NewBroker создает новый брокер
func NewBroker() *Broker {
	b := &Broker{
		topics:         make(map[string][]*Message),
		clients:        make(map[string][]string),
		messageHeap:    MinHeap{},
		unacknowledged: make(map[string]*Message),
		stopCleanup:    make(chan struct{}),
	}
	heap.Init(&b.messageHeap)
	go b.CleanUpExpiredMessages()
	return b
}

// CreateTopic создает новый топик
func (b *Broker) CreateTopic(topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.topics[topic]; exists {
		return fmt.Errorf("topic %s already exists", topic)
	}
	b.topics[topic] = []*Message{}
	return nil
}

// DeleteTopic удаляет топик
func (b *Broker) DeleteTopic(topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.topics, topic)
	return nil
}

// Subscribe подписывает клиента на топик
func (b *Broker) Subscribe(clientID, topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clients[clientID] = append(b.clients[clientID], topic)
	return nil
}

// Unsubscribe отписывает клиента от топика
func (b *Broker) Unsubscribe(clientID, topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	topics := b.clients[clientID]
	for i, t := range topics {
		if t == topic {
			b.clients[clientID] = append(topics[:i], topics[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("client %s not subscribed to topic %s", clientID, topic)
}

// Publish публикует сообщение в топик
func (b *Broker) Publish(topic string, message Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.topics[topic]; !exists {
		return fmt.Errorf("topic %s does not exist", topic)
	}

	msg := &Message{
		Content:    message.Content,
		Priority:   message.Priority,
		Expiration: message.Expiration,
		Topic:      topic,
	}
	b.topics[topic] = append(b.topics[topic], msg)

	heap.Push(&b.messageHeap, msg)
	return nil
}

func (b *Broker) PublishBatch(topic string, messages []Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Проверяем, существует ли топик
	if _, exists := b.topics[topic]; !exists {
		return fmt.Errorf("topic %s does not exist", topic)
	}

	// Обрабатываем каждое сообщение
	for _, message := range messages {
		msg := &Message{
			Content:    message.Content,
			Priority:   message.Priority,
			Expiration: message.Expiration,
			Topic:      topic,
		}

		// Добавляем сообщение в топик
		b.topics[topic] = append(b.topics[topic], msg)

		// Добавляем сообщение в MinHeap
		heap.Push(&b.messageHeap, msg)
	}

	return nil
}

// ReceiveFromTopic получает сообщение для клиента
func (b *Broker) ReceiveFromTopic(clientID string) (string, error) {
	b.mu.RLock()
	if _, exists := b.unacknowledged[clientID]; exists {
		b.mu.RUnlock()
		return "", fmt.Errorf("client %s has an unacknowledged message", clientID)
	}

	topics, subscribed := b.clients[clientID]
	if !subscribed {
		b.mu.RUnlock()
		return "", fmt.Errorf("client %s not subscribed to any topic", clientID)
	}

	for _, topic := range topics {
		if messages, exists := b.topics[topic]; exists && len(messages) > 0 {
			msg := messages[0]
			b.mu.RUnlock()

			b.mu.Lock()
			b.topics[topic] = messages[1:]
			b.unacknowledged[clientID] = msg
			b.mu.Unlock()

			return msg.Content, nil
		}
	}

	b.mu.RUnlock()
	return "", fmt.Errorf("no messages for client %s", clientID)
}

// AcknowledgeMessage подтверждает получение сообщения клиентом
func (b *Broker) AcknowledgeMessage(clientID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if msg, exists := b.unacknowledged[clientID]; exists {
		delete(b.unacknowledged, clientID)
		fmt.Printf("Message '%s' acknowledged by client '%s'\n", msg.Content, clientID)
		return nil
	}
	return fmt.Errorf("no unacknowledged message for client %s", clientID)
}

// ReceiveBatchFromTopic получает пакет сообщений для клиента из заданного топика
func (b *Broker) ReceiveBatchFromTopic(clientID, topic string, batchSize int) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.clients[clientID]; !exists {
		return nil, fmt.Errorf("client %s not subscribed to any topics", clientID)
	}

	messages, exists := b.topics[topic]
	if !exists {
		return nil, fmt.Errorf("topic %s does not exist", topic)
	}

	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages in topic %s", topic)
	}

	if batchSize > len(messages) {
		batchSize = len(messages)
	}

	batch := messages[:batchSize]
	var result []string
	for _, msg := range batch {
		result = append(result, msg.Content)
	}

	// Удаляем переданные сообщения из очереди
	b.topics[topic] = messages[batchSize:]
	return result, nil
}

// CleanUpExpiredMessages запускает фоновую очистку истекших сообщений
func (b *Broker) CleanUpExpiredMessages() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			b.mu.Lock()
			now := time.Now()
			for b.messageHeap.Len() > 0 {
				msg := b.messageHeap[0]
				if msg.Expiration.After(now) {
					break
				}
				// Удаляем из MinHeap
				heap.Pop(&b.messageHeap)

				// Удаляем из соответствующего топика
				if messages, exists := b.topics[msg.Topic]; exists {
					filtered := []*Message{}
					for _, m := range messages {
						if m != msg {
							filtered = append(filtered, m)
						}
					}
					b.topics[msg.Topic] = filtered
				}
			}
			b.mu.Unlock()
		case <-b.stopCleanup:
			return
		}
	}
}

// Close останавливает брокер и завершает фоновые задачи
func (b *Broker) Close() {
	close(b.stopCleanup)
}
