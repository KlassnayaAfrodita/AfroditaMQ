package client

import (
	"MessageBroker/broker"
)

type SimplePublisher struct {
	id     string
	broker broker.Broker
}

func NewPublisher(id string, broker broker.Broker) *SimplePublisher {
	return &SimplePublisher{id: id, broker: broker}
}

func (p *SimplePublisher) Publish(topic string, message string) error {
	return p.broker.Publish(topic, message)
}
