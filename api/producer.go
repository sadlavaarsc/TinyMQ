// Package api 提供 Producer/Consumer API
package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/sadlavaarsc/TinyMQ/broker"
)

// Producer 消息生产者
type Producer struct {
	brokerAddr string
	client     *http.Client
}

// NewProducer 创建生产者
func NewProducer(brokerAddr string) *Producer {
	return &Producer{
		brokerAddr: brokerAddr,
		client:     &http.Client{Timeout: 5 * time.Second},
	}
}

// Send 发送消息到指定 Topic
func (p *Producer) Send(topic string, payload []byte) error {
	msg := broker.Message{Topic: topic, Payload: payload, Timestamp: time.Now().Unix()}
	body, _ := json.Marshal(msg)
	resp, err := p.client.Post(p.brokerAddr+"/api/send", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("send failed: %d", resp.StatusCode)
	}
	return nil
}
