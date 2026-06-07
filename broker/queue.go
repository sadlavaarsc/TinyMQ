// Package broker 实现 TinyMQ 的核心消息路由与管理。
package broker

import (
	"fmt"
	"sync"
	"time"

	"TinyMQ/storage"
)

// Queue 代表 Topic 下的一个 Partition，是消息存储的最小单元。
type Queue struct {
	mu sync.RWMutex

	// topic 所属 Topic 名称
	topic string

	// partition 是 Partition 编号
	partition int

	// storage 是底层存储引擎
	storage *storage.Storage

	// nextOffset 是下一个待分配的 offset
	nextOffset int64
}

// NewQueue 创建一个新的 Queue（Partition）。
func NewQueue(topic string, partition int, st *storage.Storage) (*Queue, error) {
	// 从存储中恢复当前最大 offset
	maxOffset, err := st.MaxOffset(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("recover offset failed: %w", err)
	}
	return &Queue{
		topic:      topic,
		partition:  partition,
		storage:    st,
		nextOffset: maxOffset + 1,
	}, nil
}

// Produce 将消息追加到 Queue，返回分配的 offset。
func (q *Queue) Produce(payload []byte) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	offset := q.nextOffset
	timestamp := time.Now().UnixMilli()

	msg := storage.Record{
		Topic:     q.topic,
		Partition: q.partition,
		Offset:    offset,
		Payload:   payload,
		Timestamp: timestamp,
	}

	if err := q.storage.Append(msg); err != nil {
		return 0, fmt.Errorf("append message failed: %w", err)
	}

	q.nextOffset++
	return offset, nil
}

// Consume 从指定 offset 开始批量读取消息。
func (q *Queue) Consume(offset int64, maxBatch int) ([]Message, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	records, err := q.storage.Read(q.topic, q.partition, offset, maxBatch)
	if err != nil {
		return nil, fmt.Errorf("read messages failed: %w", err)
	}

	msgs := make([]Message, 0, len(records))
	for _, r := range records {
		msgs = append(msgs, Message{
			Topic:     r.Topic,
			Partition: r.Partition,
			Offset:    r.Offset,
			Payload:   r.Payload,
			Timestamp: r.Timestamp,
		})
	}
	return msgs, nil
}

// NextOffset 返回下一个待分配的 offset。
func (q *Queue) NextOffset() int64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.nextOffset
}
