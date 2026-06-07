// Package broker 实现 TinyMQ 的核心消息路由与管理。
// 单 Broker 架构，负责 Topic、Queue、Consumer Group 的生命周期管理。
package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"TinyMQ/storage"
)

// Broker 是 TinyMQ 的单节点核心，管理所有 Topic 与 Consumer Group。
type Broker struct {
	mu sync.RWMutex

	// topics 维护所有主题
	topics map[string]*Topic

	// consumerGroups 维护所有消费者组
	consumerGroups map[string]*ConsumerGroup

	// storage 是底层存储引擎
	storage *storage.Storage

	// ctx 用于优雅退出
	ctx    context.Context
	cancel context.CancelFunc
}

// NewBroker 创建一个新的 Broker 实例。
func NewBroker(st *storage.Storage) *Broker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Broker{
		topics:         make(map[string]*Topic),
		consumerGroups: make(map[string]*ConsumerGroup),
		storage:        st,
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Start 启动 Broker 的后台任务（如 ACK 超时检查）。
func (b *Broker) Start() {
	go b.retryLoop()
}

// Stop 优雅关闭 Broker。
func (b *Broker) Stop() {
	b.cancel()
}

// CreateTopic 创建 Topic，如果不存在则初始化。
func (b *Broker) CreateTopic(name string, partitionNum int) (*Topic, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if t, ok := b.topics[name]; ok {
		return t, nil
	}

	t, err := NewTopic(name, partitionNum, b.storage)
	if err != nil {
		return nil, fmt.Errorf("create topic %s failed: %w", name, err)
	}
	b.topics[name] = t
	return t, nil
}

// GetTopic 获取指定 Topic。
func (b *Broker) GetTopic(name string) (*Topic, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.topics[name]
	return t, ok
}

// CreateConsumerGroup 创建或获取 Consumer Group。
func (b *Broker) CreateConsumerGroup(groupID, topicName string) (*ConsumerGroup, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if cg, ok := b.consumerGroups[groupID]; ok {
		return cg, nil
	}

	topic, ok := b.topics[topicName]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", topicName)
	}

	cg := NewConsumerGroup(groupID, topic)
	b.consumerGroups[groupID] = cg
	return cg, nil
}

// GetConsumerGroup 获取指定 Consumer Group。
func (b *Broker) GetConsumerGroup(groupID string) (*ConsumerGroup, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	cg, ok := b.consumerGroups[groupID]
	return cg, ok
}

// Produce 发送消息到指定 Topic 的某个 Partition。
func (b *Broker) Produce(topicName string, partition int, payload []byte) (int64, error) {
	topic, ok := b.GetTopic(topicName)
	if !ok {
		return 0, fmt.Errorf("topic %s not found", topicName)
	}
	return topic.Produce(partition, payload)
}

// Consume 从 Consumer Group 拉取消息。
func (b *Broker) Consume(groupID string, maxBatch int) ([]Message, error) {
	b.mu.RLock()
	cg, ok := b.consumerGroups[groupID]
	b.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("consumer group %s not found", groupID)
	}
	return cg.Consume(maxBatch)
}

// Ack 确认消息已消费。
func (b *Broker) Ack(groupID string, msg Message) error {
	b.mu.RLock()
	cg, ok := b.consumerGroups[groupID]
	b.mu.RUnlock()
	if !ok {
		return fmt.Errorf("consumer group %s not found", groupID)
	}
	return cg.Ack(msg)
}

// retryLoop 后台循环：检查未 ACK 消息并重新投递。
func (b *Broker) retryLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.mu.RLock()
			groups := make([]*ConsumerGroup, 0, len(b.consumerGroups))
			for _, cg := range b.consumerGroups {
				groups = append(groups, cg)
			}
			b.mu.RUnlock()

			for _, cg := range groups {
				cg.RetryPendingMessages()
			}
		case <-b.ctx.Done():
			return
		}
	}
}

// Message 是 TinyMQ 的消息结构。
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Payload   []byte
	Timestamp int64
}
