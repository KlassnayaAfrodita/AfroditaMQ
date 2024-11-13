package main

import (
	"MessageBroker/broker"
	"MessageBroker/handlers"
	"fmt"
	"log"
	"net/http"
)

func main() {
	// Инициализируем брокер
	broker := broker.NewBroker()

	// Инициализируем обработчики
	http.HandleFunc("/subscribe", handlers.SubscribeHandler(broker))
	http.HandleFunc("/unsubscribe", handlers.UnsubscribeHandler(broker))
	http.HandleFunc("/create-topic", handlers.CreateTopicHandler(broker))
	http.HandleFunc("/publish", handlers.PublishHandler(broker))

	// Запускаем сервер на порту 8080
	fmt.Println("Starting server on port 8080...")
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("Error starting server: ", err)
	}
}
