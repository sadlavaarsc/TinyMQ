// Package api 暴露 TinyMQ 的 Producer 与 Consumer HTTP 接口，同时提供客户端 SDK。
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"TinyMQ/broker"
)

// ConsumerHandler 处理消费者相关的 HTTP 请求（服务端）。
type ConsumerHandler struct {
	broker *broker.Broker
}

// NewConsumerHandler 创建 ConsumerHandler。
func NewConsumerHandler(b *broker.Broker) *ConsumerHandler {
	return &ConsumerHandler{broker: b}
}

// PullRequest 是拉取消息的请求体。
type PullRequest struct {
	GroupID  string `json:"group_id"`
	MaxBatch int    `json:"max_batch"`
}

// PullResponse 是拉取消息的响应体。
type PullResponse struct {
	Messages []broker.Message `json:"messages"`
	Error    string           `json:"error,omitempty"`
}

// AckRequest 是确认消息的请求体。
type AckRequest struct {
	GroupID string         `json:"group_id"`
	Message broker.Message `json:"message"`
}

// AckResponse 是确认消息的响应体。
type AckResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// Pull 处理消息拉取请求：POST /consumer/pull。
func (h *ConsumerHandler) Pull(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PullRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("decode request failed: %v", err), http.StatusBadRequest)
		return
	}

	if req.GroupID == "" {
		http.Error(w, "group_id is required", http.StatusBadRequest)
		return
	}
	if req.MaxBatch <= 0 {
		req.MaxBatch = 10
	}

	msgs, err := h.broker.Consume(req.GroupID, req.MaxBatch)
	resp := PullResponse{Messages: msgs}
	if err != nil {
		resp.Error = err.Error()
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// Ack 处理消息确认请求：POST /consumer/ack。
func (h *ConsumerHandler) Ack(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req AckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("decode request failed: %v", err), http.StatusBadRequest)
		return
	}

	if req.GroupID == "" {
		http.Error(w, "group_id is required", http.StatusBadRequest)
		return
	}

	err := h.broker.Ack(req.GroupID, req.Message)
	resp := AckResponse{Success: err == nil}
	if err != nil {
		resp.Error = err.Error()
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// JoinGroup 处理消费者加入组的请求：POST /consumer/join?group_id=xxx&topic=xxx。
func (h *ConsumerHandler) JoinGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	groupID := r.URL.Query().Get("group_id")
	topicName := r.URL.Query().Get("topic")
	if groupID == "" || topicName == "" {
		http.Error(w, "missing group_id or topic", http.StatusBadRequest)
		return
	}

	// 确保 Topic 存在
	topic, ok := h.broker.GetTopic(topicName)
	if !ok {
		_, err := h.broker.CreateTopic(topicName, 2)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		topic, _ = h.broker.GetTopic(topicName)
	}

	// 创建 Consumer Group
	_, err := h.broker.CreateConsumerGroup(groupID, topicName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("joined group %s for topic %s", groupID, topicName)))
	_ = topic
}

// GetOffset 处理查询消费进度的请求：GET /consumer/offset?group_id=xxx&partition=0。
func (h *ConsumerHandler) GetOffset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	groupID := r.URL.Query().Get("group_id")
	partitionStr := r.URL.Query().Get("partition")
	if groupID == "" || partitionStr == "" {
		http.Error(w, "missing group_id or partition", http.StatusBadRequest)
		return
	}

	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		http.Error(w, "invalid partition", http.StatusBadRequest)
		return
	}

	cg, ok := h.broker.GetConsumerGroup(groupID)
	if !ok {
		http.Error(w, "consumer group not found", http.StatusNotFound)
		return
	}

	offset := cg.GetOffset(partition)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"group_id":  groupID,
		"partition": partition,
		"offset":    offset,
	})
}

// Consumer 是消费者客户端 SDK，用于从 Broker 拉取消息。
type Consumer struct {
	brokerAddr string
	groupID    string
	topic      string
	client     *http.Client
}

// NewConsumer 创建消费者客户端。
func NewConsumer(brokerAddr, groupID, topic string) *Consumer {
	return &Consumer{
		brokerAddr: brokerAddr,
		groupID:    groupID,
		topic:      topic,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

// Pull 从 Broker 拉取消息。
func (c *Consumer) Pull(batchSize int) ([]broker.Message, error) {
	url := fmt.Sprintf("%s/api/pull?topic=%s&group=%s&batch=%d", c.brokerAddr, c.topic, c.groupID, batchSize)
	resp, err := c.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pull failed: %d", resp.StatusCode)
	}
	var msgs []broker.Message
	if err := json.NewDecoder(resp.Body).Decode(&msgs); err != nil {
		return nil, err
	}
	return msgs, nil
}

// Ack 确认消息已消费。
func (c *Consumer) Ack(offset int64) error {
	url := fmt.Sprintf("%s/api/ack?topic=%s&group=%s&offset=%d", c.brokerAddr, c.topic, c.groupID, offset)
	resp, err := c.client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}
