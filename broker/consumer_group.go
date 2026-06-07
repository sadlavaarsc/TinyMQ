// Package broker 实现 TinyMQ 的核心消息路由与管理。
package broker

import (
	"fmt"
	"sync"
	"time"
)

// ConsumerGroup 管理一组消费者，对 Partition 进行负载均衡，并处理 ACK 与超时重试。
type ConsumerGroup struct {
	mu sync.RWMutex

	// groupID 是消费者组唯一标识
	groupID string

	// topic 是订阅的 Topic
	topic *Topic

	// offsets 记录每个 Partition 当前消费到的 offset
	offsets map[int]int64

	// pending 记录已投递但未 ACK 的消息，用于超时重试
	pending map[string]PendingMessage

	// consumers 记录组内消费者（简化模型，仅做计数与分配）
	consumers []string
}

// PendingMessage 代表一条已投递待确认的消息。
type PendingMessage struct {
	Message   Message
	DeliverAt time.Time
	RetryCount int
}

// NewConsumerGroup 创建 ConsumerGroup。
func NewConsumerGroup(groupID string, topic *Topic) *ConsumerGroup {
	num := topic.NumPartitions()
	offsets := make(map[int]int64, num)
	for i := 0; i < num; i++ {
		offsets[i] = 0
	}

	return &ConsumerGroup{
		groupID:   groupID,
		topic:     topic,
		offsets:   offsets,
		pending:   make(map[string]PendingMessage),
		consumers: make([]string, 0),
	}
}

// Join 消费者加入组。
func (cg *ConsumerGroup) Join(consumerID string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	cg.consumers = append(cg.consumers, consumerID)
}

// Leave 消费者离开组。
func (cg *ConsumerGroup) Leave(consumerID string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	for i, id := range cg.consumers {
		if id == consumerID {
			cg.consumers = append(cg.consumers[:i], cg.consumers[i+1:]...)
			break
		}
	}
}

// Consume 从所有 Partition 拉取消息，简单轮询策略。
func (cg *ConsumerGroup) Consume(maxBatch int) ([]Message, error) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if len(cg.consumers) == 0 {
		return nil, fmt.Errorf("no consumer in group %s", cg.groupID)
	}

	var result []Message
	numPartitions := cg.topic.NumPartitions()

	for i := 0; i < numPartitions; i++ {
		offset := cg.offsets[i]
		msgs, err := cg.topic.Consume(i, offset, maxBatch)
		if err != nil {
			continue
		}
		for _, msg := range msgs {
			key := fmt.Sprintf("%s-%d-%d", msg.Topic, msg.Partition, msg.Offset)
			cg.pending[key] = PendingMessage{
				Message:    msg,
				DeliverAt:  time.Now(),
				RetryCount: 0,
			}
			cg.offsets[i] = msg.Offset + 1
			result = append(result, msg)
			if len(result) >= maxBatch {
				return result, nil
			}
		}
	}
	return result, nil
}

// Ack 确认消息已消费，从 pending 中移除。
func (cg *ConsumerGroup) Ack(msg Message) error {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	key := fmt.Sprintf("%s-%d-%d", msg.Topic, msg.Partition, msg.Offset)
	if _, ok := cg.pending[key]; !ok {
		return fmt.Errorf("message %s not in pending", key)
	}
	delete(cg.pending, key)
	return nil
}

// RetryPendingMessages 检查 pending 中超时的消息并重新投递。
func (cg *ConsumerGroup) RetryPendingMessages() {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	now := time.Now()
	for key, pm := range cg.pending {
		if now.Sub(pm.DeliverAt) > 30*time.Second {
			// 超时重试：更新投递时间，增加重试计数
			pm.DeliverAt = now
			pm.RetryCount++
			cg.pending[key] = pm

			// 实际场景中此处会触发重新投递逻辑
			// 简化模型：仅更新状态，由下次 Consume 读取
			if pm.RetryCount > 3 {
				// 超过最大重试次数，可进入死信队列
				delete(cg.pending, key)
			}
		}
	}
}

// GetOffset 获取指定 Partition 的消费进度。
func (cg *ConsumerGroup) GetOffset(partition int) (int64, bool) {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	off, ok := cg.offsets[partition]
	return off, ok
}

// SetOffset 设置指定 Partition 的消费进度。
func (cg *ConsumerGroup) SetOffset(partition int, offset int64) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	cg.offsets[partition] = offset
}
