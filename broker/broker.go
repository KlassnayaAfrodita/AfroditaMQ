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

type Broker interface {
	Publish(topic string, message Message) error
	Subscribe(clientID string, topic string) error
	Unsubscribe(clientID string, topic string) error
	CreateTopic(topic string) error
	DeleteTopic(topic string) error
	ReceiveFromTopic(clientID string) (string, error)
	CleanUpExpiredMessages()
	AcknowledgeMessage(clientID string) error
}

// SimpleBroker реализует интерфейс брокера
type SimpleBroker struct {
	topics         map[string][]*Message
	clients        map[string][]string
	messageHeap    MinHeap
	publishChan    chan Message
	mu             sync.RWMutex
	unacknowledged map[string]*Message
	stopChan       chan struct{}
}

// NewBroker создает новый брокер
func NewBroker() *SimpleBroker {
	b := &SimpleBroker{
		topics:         make(map[string][]*Message),
		clients:        make(map[string][]string),
		messageHeap:    MinHeap{},
		publishChan:    make(chan Message, 100),
		unacknowledged: make(map[string]*Message),
		stopChan:       make(chan struct{}),
	}
	heap.Init(&b.messageHeap)
	go b.handlePublications()
	go b.CleanUpExpiredMessages()
	return b
}

// CreateTopic создает новый топик
func (b *SimpleBroker) CreateTopic(topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.topics[topic]; exists {
		return fmt.Errorf("topic %s already exists", topic)
	}
	b.topics[topic] = []*Message{}
	return nil
}

// DeleteTopic удаляет топик
func (b *SimpleBroker) DeleteTopic(topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.topics, topic)
	return nil
}

// Subscribe подписывает клиента на топик
func (b *SimpleBroker) Subscribe(clientID string, topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clients[clientID] = append(b.clients[clientID], topic)
	return nil
}

// Unsubscribe отписывает клиента от топика
func (b *SimpleBroker) Unsubscribe(clientID string, topic string) error {
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
func (b *SimpleBroker) Publish(topic string, message Message) error {
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

// AsyncPublish отправляет сообщение через канал
func (b *SimpleBroker) AsyncPublish(topic string, message Message) {
	b.publishChan <- message
}

// handlePublications обрабатывает публикации
func (b *SimpleBroker) handlePublications() {
	for {
		select {
		case msg := <-b.publishChan:
			_ = b.Publish(msg.Topic, msg)
		case <-b.stopChan:
			return
		}
	}
}

// ReceiveFromTopic получает сообщение для клиента
func (b *SimpleBroker) ReceiveFromTopic(clientID string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, hasUnack := b.unacknowledged[clientID]; hasUnack {
		return "", fmt.Errorf("client %s has an unacknowledged message", clientID)
	}

	topics := b.clients[clientID]
	for _, topic := range topics {
		if messages, exists := b.topics[topic]; exists && len(messages) > 0 {
			msg := messages[0]
			b.topics[topic] = messages[1:]
			b.unacknowledged[clientID] = msg
			return msg.Content, nil
		}
	}
	return "", fmt.Errorf("no messages for client %s", clientID)
}

// AcknowledgeMessage подтверждает получение сообщения
func (b *SimpleBroker) AcknowledgeMessage(clientID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if msg, exists := b.unacknowledged[clientID]; exists {
		delete(b.unacknowledged, clientID)
		fmt.Printf("Message '%s' acknowledged by client '%s'\n", msg.Content, clientID)
		return nil
	}
	return fmt.Errorf("no unacknowledged message for client %s", clientID)
}

// startCleanUpWorker запускает очистку
func (b *SimpleBroker) CleanUpExpiredMessages() {
	for {
		select {
		case <-time.After(100 * time.Millisecond):
			b.mu.Lock()
			now := time.Now()
			for b.messageHeap.Len() > 0 {
				msg := b.messageHeap[0]
				if msg.Expiration.After(now) {
					break
				}
				heap.Pop(&b.messageHeap)
				b.removeMessageFromTopic(msg)
			}
			b.mu.Unlock()
		case <-b.stopChan:
			return
		}
	}
}

// removeMessageFromTopic удаляет сообщение из топика
func (b *SimpleBroker) removeMessageFromTopic(msg *Message) {
	messages := b.topics[msg.Topic]
	for i, m := range messages {
		if m == msg {
			b.topics[msg.Topic] = append(messages[:i], messages[i+1:]...)
			return
		}
	}
}

// Stop завершает работу брокера
func (b *SimpleBroker) Stop() {
	close(b.stopChan)
}
