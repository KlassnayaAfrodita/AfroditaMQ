package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// ClientSubscriber реализует интерфейс Subscriber
type ClientSubscriber struct {
	ClientID string
	BaseURL  string
}

// NewSubscriber создает новый экземпляр ClientSubscriber
func NewClientSubscriber(clientID string, baseURL string) *ClientSubscriber {
	return &ClientSubscriber{
		ClientID: clientID,
		BaseURL:  baseURL,
	}
}

// Subscribe подписывает клиента на указанный топик
func (s *ClientSubscriber) Subscribe(topic string) error {
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
func (s *ClientSubscriber) Unsubscribe(topic string) error {
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

// ReceiveMessage получает сообщение из топика
func (s *ClientSubscriber) ReceiveMessage() (string, error) {
	url := fmt.Sprintf("%s/receive?client_id=%s", s.BaseURL, s.ClientID)
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to receive message: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to receive message, status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %v", err)
	}

	return string(body), nil
}

// ReceiveBatchFromTopic получает пакет сообщений
func (s *ClientSubscriber) ReceiveBatchFromTopic(topic string, batchSize int) ([]string, error) {
	url := fmt.Sprintf("%s/receive_batch", s.BaseURL)
	batchRequest := map[string]interface{}{
		"client_id":  s.ClientID,
		"topic":      topic,
		"batch_size": batchSize,
	}

	data, err := json.Marshal(batchRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal batch request: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return nil, fmt.Errorf("failed to send batch request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to receive batch, status code: %d", resp.StatusCode)
	}

	var messages []string
	if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
		return nil, fmt.Errorf("failed to decode batch response: %v", err)
	}

	return messages, nil
}

// AcknowledgeMessage подтверждает получение сообщения
func (s *ClientSubscriber) AcknowledgeMessage() error {
	url := fmt.Sprintf("%s/acknowledge", s.BaseURL)
	ackRequest := map[string]string{
		"client_id": s.ClientID,
	}

	data, err := json.Marshal(ackRequest)
	if err != nil {
		return fmt.Errorf("failed to marshal acknowledge request: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("failed to send acknowledge request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to acknowledge message, status code: %d", resp.StatusCode)
	}

	fmt.Printf("Client %s acknowledged the message\n", s.ClientID)
	return nil
}
