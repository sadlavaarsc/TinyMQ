// Package api 暴露 TinyMQ 的 Producer 与 Consumer HTTP 接口，同时提供客户端 SDK。
package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"TinyMQ/broker"
)

// ProducerHandler 处理生产者相关的 HTTP 请求（服务端）。
type ProducerHandler struct {
	broker *broker.Broker
}

// NewProducerHandler 创建 ProducerHandler。
func NewProducerHandler(b *broker.Broker) *ProducerHandler {
	return &ProducerHandler{broker: b}
}

// SendRequest 是发送消息的请求体。
type SendRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Payload   string `json:"payload"`
}

// SendResponse 是发送消息的响应体。
type SendResponse struct {
	Offset int64  `json:"offset"`
	Error  string `json:"error,omitempty"`
}

// Send 处理消息发送请求：POST /producer/send。
func (h *ProducerHandler) Send(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SendRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("decode request failed: %v", err), http.StatusBadRequest)
		return
	}

	offset, err := h.broker.Produce(req.Topic, req.Partition, []byte(req.Payload))
	resp := SendResponse{Offset: offset}
	if err != nil {
		resp.Error = err.Error()
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// CreateTopic 处理创建 Topic 请求：POST /producer/topic?topic=xxx&partitions=2。
func (h *ProducerHandler) CreateTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topicName := r.URL.Query().Get("topic")
	partitionStr := r.URL.Query().Get("partitions")
	if topicName == "" || partitionStr == "" {
		http.Error(w, "missing topic or partitions", http.StatusBadRequest)
		return
	}

	partitions, err := strconv.Atoi(partitionStr)
	if err != nil {
		http.Error(w, "invalid partitions", http.StatusBadRequest)
		return
	}

	_, err = h.broker.CreateTopic(topicName, partitions)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(fmt.Sprintf("topic %s created with %d partitions", topicName, partitions)))
}

// Producer 是生产者客户端 SDK，用于向 Broker 发送消息。
type Producer struct {
	brokerAddr string
	client     *http.Client
}

// NewProducer 创建生产者客户端。
func NewProducer(brokerAddr string) *Producer {
	return &Producer{
		brokerAddr: brokerAddr,
		client:     &http.Client{Timeout: 5 * time.Second},
	}
}

// Send 发送消息到指定 Topic。
func (p *Producer) Send(topic string, payload []byte) error {
	msg := broker.Message{Topic: topic, Payload: payload, Timestamp: time.Now().UnixMilli()}
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
