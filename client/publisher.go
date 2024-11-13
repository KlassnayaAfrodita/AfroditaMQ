package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

// SimplePublisher реализует интерфейс Publisher
type SimplePublisher struct {
	ClientID string
	BaseURL  string
}

// NewPublisher создает новый экземпляр SimplePublisher
func NewPublisher(clientID string, baseURL string) *SimplePublisher {
	return &SimplePublisher{
		ClientID: clientID,
		BaseURL:  baseURL,
	}
}

// Publish публикует сообщение на указанный топик с приоритетом и TTL
func (p *SimplePublisher) Publish(topic string, message string, priority int, ttl int64) error {
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
