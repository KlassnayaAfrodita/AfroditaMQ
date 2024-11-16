package main

import (
	"MessageBroker/broker"
	"MessageBroker/handlers"
	"fmt"
	"net/http"
)

func main() {
	// Создание нового брокера
	b := broker.NewBroker()

	// Настройка маршрутов
	http.HandleFunc("/create_topic", handlers.CreateTopicHandler(b))
	http.HandleFunc("/subscribe", handlers.SubscribeHandler(b))
	http.HandleFunc("/unsubscribe", handlers.UnsubscribeHandler(b))
	http.HandleFunc("/publish", handlers.PublishHandler(b))
	http.HandleFunc("/receive", handlers.ReceiveMessageHandler(b))
	http.HandleFunc("/acknowledge", handlers.AcknowledgeHandler(b))

	// Запуск сервера
	address := ":8080"
	fmt.Printf("Server running on %s\n", address)
	if err := http.ListenAndServe(address, nil); err != nil {
		panic(fmt.Sprintf("Failed to start server: %v", err))
	}
}
