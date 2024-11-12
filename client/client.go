package client

type Publisher interface {
	Publish(topic string, message string) error
}

type Subscriber interface {
	Subscribe(topic string) error
	ReceiveMessage() error
}
