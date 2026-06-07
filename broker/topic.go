// Package broker 实现 TinyMQ 的核心消息路由与管理。
package broker

import (
	"fmt"
	"sync"

	"TinyMQ/storage"
)

// Topic 代表一个消息主题，内部包含多个 Queue（Partition）。
type Topic struct {
	mu sync.RWMutex

	// name 是 Topic 名称
	name string

	// queues 是 Topic 下的所有 Partition
	queues []*Queue

	// storage 是底层存储引擎
	storage *storage.Storage
}

// NewTopic 创建 Topic，初始化指定数量的 Partition。
func NewTopic(name string, partitionNum int, st *storage.Storage) (*Topic, error) {
	queues := make([]*Queue, 0, partitionNum)
	for i := 0; i < partitionNum; i++ {
		q, err := NewQueue(name, i, st)
		if err != nil {
			return nil, fmt.Errorf("init queue %d failed: %w", i, err)
		}
		queues = append(queues, q)
	}
	return &Topic{
		name:    name,
		queues:  queues,
		storage: st,
	}, nil
}

// Name 返回 Topic 名称。
func (t *Topic) Name() string {
	return t.name
}

// NumPartitions 返回 Partition 数量。
func (t *Topic) NumPartitions() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.queues)
}

// GetQueue 获取指定 Partition 的 Queue。
func (t *Topic) GetQueue(partition int) (*Queue, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if partition < 0 || partition >= len(t.queues) {
		return nil, fmt.Errorf("partition %d out of range", partition)
	}
	return t.queues[partition], nil
}

// Produce 发送消息到指定 Partition。
func (t *Topic) Produce(partition int, payload []byte) (int64, error) {
	q, err := t.GetQueue(partition)
	if err != nil {
		return 0, err
	}
	return q.Produce(payload)
}

// Consume 从指定 Partition 的 offset 开始消费。
func (t *Topic) Consume(partition int, offset int64, maxBatch int) ([]Message, error) {
	q, err := t.GetQueue(partition)
	if err != nil {
		return nil, err
	}
	return q.Consume(offset, maxBatch)
}
