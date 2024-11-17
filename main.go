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
	http.HandleFunc("/create_topic", handlers.CreateTopicHandler(b))   // Создание топика
	http.HandleFunc("/subscribe", handlers.SubscribeHandler(b))        // Подписка на топик
	http.HandleFunc("/unsubscribe", handlers.UnsubscribeHandler(b))    // Отписка от топика
	http.HandleFunc("/publish", handlers.PublishHandler(b))            // Публикация сообщения
	http.HandleFunc("/publish_batch", handlers.PublishBatchHandler(b)) // Пакетная публикация сообщений
	http.HandleFunc("/receive", handlers.ReceiveMessageHandler(b))     // Получение сообщения для клиента
	http.HandleFunc("/receive_batch", handlers.ReceiveBatchHandler(b)) // Получение пакета сообщений для клиента
	http.HandleFunc("/acknowledge", handlers.AcknowledgeHandler(b))    // Подтверждение получения сообщения

	// Запуск сервера
	address := ":8080"
	fmt.Printf("Server running on %s\n", address)
	if err := http.ListenAndServe(address, nil); err != nil {
		panic(fmt.Sprintf("Failed to start server: %v", err))
	}
}
