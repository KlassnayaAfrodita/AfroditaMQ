package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// ClientPublisher реализует интерфейс ClientPublisher
type ClientPublisher struct {
	ClientID string
	BaseURL  string
}

// NewClientPublisher создает новый экземпляр ClientPublisher
func NewClientPublisher(clientID string, baseURL string) *ClientPublisher {
	return &ClientPublisher{
		ClientID: clientID,
		BaseURL:  baseURL,
	}
}

// Publish публикует сообщение на указанный топик
func (p *ClientPublisher) Publish(topic, message string, priority int, ttl int64) error {
	url := fmt.Sprintf("%s/publish", p.BaseURL)
	publishRequest := map[string]interface{}{
		"topic":    topic,
		"message":  message,
		"priority": priority,
		"ttl":      ttl,
	}

	data, err := json.Marshal(publishRequest)
	if err != nil {
		return fmt.Errorf("failed to marshal publish request: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("failed to send publish request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to publish message, status code: %d", resp.StatusCode)
	}

	fmt.Printf("Message published to topic %s: %s\n", topic, message)
	return nil
}

// PublishBatch публикует пакет сообщений на указанный топик
func (p *ClientPublisher) PublishBatch(topic string, messages []map[string]interface{}) error {
	url := fmt.Sprintf("%s/publish_batch", p.BaseURL)
	publishBatchRequest := map[string]interface{}{
		"topic":    topic,
		"messages": messages,
	}

	data, err := json.Marshal(publishBatchRequest)
	if err != nil {
		return fmt.Errorf("failed to marshal publish batch request: %v", err)
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return fmt.Errorf("failed to send publish batch request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to publish batch, status code: %d", resp.StatusCode)
	}

	fmt.Printf("Batch of messages published to topic %s\n", topic)
	return nil
}
