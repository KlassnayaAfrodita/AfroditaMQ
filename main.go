package main

import (
	"MessageBroker/broker"
	"MessageBroker/client"
)

func main() {
	b := broker.NewBroker()

	// Создаем издателя и подписчиков
	publisher := client.NewPublisher("publisher", b)
	subscriber1 := client.NewSubscriber("subscriber1", b, "tech")
	subscriber2 := client.NewSubscriber("subscriber2", b, "sports")

	// Подписчики подписываются на топики
	subscriber1.Subscribe("tech")
	subscriber2.Subscribe("sports")

	// Издатель публикует сообщения
	publisher.Publish("tech", "New Go version released!")
	publisher.Publish("sports", "Local team wins championship!")

	// Подписчики получают сообщения
	subscriber1.ReceiveMessage()
	subscriber2.ReceiveMessage()
}
