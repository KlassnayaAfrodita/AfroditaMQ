package broker

import "fmt"

func (b *SimpleBroker) Publish(topic string, message string) error {
	b.topics[topic] = append(b.topics[topic], message)
	fmt.Printf("Message published to topic %s: %s\n", topic, message)
	return nil
}

func (b *SimpleBroker) Subscribe(clientID string, topic string) error {
	b.clients[clientID] = append(b.clients[clientID], topic)
	fmt.Printf("Client %s subscribed to topic %s\n", clientID, topic)
	return nil
}

func (b *SimpleBroker) ReceiveFromTopic(clientID string) (string, error) {
	topics := b.clients[clientID]
	for _, topic := range topics {
		if len(b.topics[topic]) > 0 {
			message := b.topics[topic][0]
			b.topics[topic] = b.topics[topic][1:]
			return message, nil
		}
	}
	return "", fmt.Errorf("no messages in subscribed topics")
}
