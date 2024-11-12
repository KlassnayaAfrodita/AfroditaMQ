package client

import (
	"MessageBroker/broker"
	"fmt"
)

type SimpleSubscriber struct {
	id     string
	broker broker.Broker
}

func NewSubscriber(id string, broker broker.Broker, topic string) *SimpleSubscriber {
	s := &SimpleSubscriber{id: id, broker: broker}
	s.Subscribe(topic)
	return s
}

func (s *SimpleSubscriber) Subscribe(topic string) error {
	return s.broker.Subscribe(s.id, topic)
}

func (s *SimpleSubscriber) ReceiveMessage() error {
	message, err := s.broker.ReceiveFromTopic(s.id)
	if err != nil {
		return err
	}
	fmt.Printf("%s received message: %s\n", s.id, message)
	return nil
}
