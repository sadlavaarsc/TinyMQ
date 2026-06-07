package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/sadlavaarsc/TinyMQ/broker"
)

// Consumer 消息消费者
type Consumer struct {
	brokerAddr string
	groupID    string
	topic      string
	client     *http.Client
}

// NewConsumer 创建消费者
func NewConsumer(brokerAddr, groupID, topic string) *Consumer {
	return &Consumer{
		brokerAddr: brokerAddr,
		groupID:    groupID,
		topic:      topic,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

// Pull 拉取消息
func (c *Consumer) Pull(batchSize int) ([]*broker.Message, error) {
	url := fmt.Sprintf("%s/api/pull?topic=%s&group=%s&batch=%d", c.brokerAddr, c.topic, c.groupID, batchSize)
	resp, err := c.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pull failed: %d", resp.StatusCode)
	}
	var msgs []*broker.Message
	if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
		return nil, err
	}
	return msgs, nil
}

// Ack 确认消息消费
func (c *Consumer) Ack(offset int64) error {
	url := fmt.Sprintf("%s/api/ack?topic=%s&group=%s&offset=%d", c.brokerAddr, c.topic, c.groupID, offset)
	resp, err := c.client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
