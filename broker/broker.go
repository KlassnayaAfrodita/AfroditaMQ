package broker

import "fmt"

type Broker interface {
	SendQueue(clientID string, message string) error
	ReceiveQueue(clientID string) (string, error)
	Publish(topic string, message string) error
	Subscribe(clientID string, topic string) error
	ReceiveFromTopic(clientID string) (string, error)
}

type SimpleBroker struct {
	queues  map[string][]string
	topics  map[string][]string
	clients map[string][]string
}

func NewBroker() *SimpleBroker {
	return &SimpleBroker{
		queues:  make(map[string][]string),
		topics:  make(map[string][]string),
		clients: make(map[string][]string),
	}
}

func (b *SimpleBroker) SendQueue(clientID string, message string) error {
	b.queues[clientID] = append(b.queues[clientID], message)
	fmt.Printf("Message sent to queue %s: %s\n", clientID, message)
	return nil
}

func (b *SimpleBroker) ReceiveQueue(clientID string) (string, error) {
	queue := b.queues[clientID]
	if len(queue) == 0 {
		return "", fmt.Errorf("no messages")
	}
	message := queue[0]
	b.queues[clientID] = queue[1:]
	return message, nil
}
