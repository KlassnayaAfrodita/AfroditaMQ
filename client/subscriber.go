package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

// SimpleSubscriber реализует интерфейс Subscriber
type SimpleSubscriber struct {
	ClientID string
	BaseURL  string
}

// NewSubscriber создает новый экземпляр SimpleSubscriber
func NewSubscriber(clientID string, baseURL string) *SimpleSubscriber {
	return &SimpleSubscriber{
		ClientID: clientID,
		BaseURL:  baseURL,
	}
}

// Subscribe подписывает клиента на указанный топик
func (s *SimpleSubscriber) Subscribe(topic string) error {
	url := fmt.Sprintf("%s/subscribe", s.BaseURL)
	subscribeRequest := map[string]string{
		"client_id": s.ClientID,
		"topic":     topic,
	}

	data, err := json.Marshal(subscribeRequest)
	if err != nil {
		return fmt.Errorf("failed to marshal subscribe request: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("failed to send subscribe request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to subscribe, status code: %d", resp.StatusCode)
	}

	fmt.Printf("Client %s subscribed to topic %s\n", s.ClientID, topic)
	return nil
}

// Unsubscribe отписывает клиента от указанного топика
func (s *SimpleSubscriber) Unsubscribe(topic string) error {
	url := fmt.Sprintf("%s/unsubscribe", s.BaseURL)
	unsubscribeRequest := map[string]string{
		"client_id": s.ClientID,
		"topic":     topic,
	}

	data, err := json.Marshal(unsubscribeRequest)
	if err != nil {
		return fmt.Errorf("failed to marshal unsubscribe request: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("failed to send unsubscribe request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to unsubscribe, status code: %d", resp.StatusCode)
	}

	fmt.Printf("Client %s unsubscribed from topic %s\n", s.ClientID, topic)
	return nil
}

// ReceiveMessage получает сообщение из топика для подписчика
func (s *SimpleSubscriber) ReceiveMessage() (string, error) {
	url := fmt.Sprintf("%s/receive", s.BaseURL)
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to receive message: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to receive message, status code: %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %v", err)
	}

	return string(body), nil
}
