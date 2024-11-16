package client

// Publisher интерфейс для публикации сообщений
type Publisher interface {
	Publish(topic string, message string, priority int, ttl int64) error
}

// Subscriber интерфейс для подписки на топики и получения сообщений
type Subscriber interface {
	Subscribe(topic string) error
	Unsubscribe(topic string) error
	ReceiveMessage() (string, error)
	AcknowledgeMessage() error
}
