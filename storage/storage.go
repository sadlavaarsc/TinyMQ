// Package storage 实现 TinyMQ 的底层存储引擎。
// 采用索引与数据分离的设计：LevelDB 存储索引（offset -> 文件位置），消息内容写入顺序日志文件。
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

// Record 代表一条消息记录。
type Record struct {
	Topic     string
	Partition int
	Offset    int64
	Payload   []byte
	Timestamp int64
}

// Storage 是 TinyMQ 的存储引擎，组合索引与日志。
type Storage struct {
	mu sync.RWMutex

	// db 是 LevelDB 实例，存储消息索引
	db *leveldb.DB

	// logDir 是日志文件目录
	logDir string

	// logWriter 是当前活跃的日志文件写入器
	logWriter *LogWriter

	// index 维护 topic-partition -> maxOffset 的映射
	index *Index
}

// NewStorage 创建 Storage 实例，打开 LevelDB 并初始化日志目录。
func NewStorage(dataDir string) (*Storage, error) {
	dbPath := filepath.Join(dataDir, "index")
	db, err := leveldb.OpenFile(dbPath, nil)
	if err != nil {
		return nil, fmt.Errorf("open leveldb failed: %w", err)
	}

	logDir := filepath.Join(dataDir, "logs")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("create log dir failed: %w", err)
	}

	lw, err := NewLogWriter(logDir)
	if err != nil {
		return nil, fmt.Errorf("init log writer failed: %w", err)
	}

	idx := NewIndex(db)
	return &Storage{
		db:        db,
		logDir:    logDir,
		logWriter: lw,
		index:     idx,
	}, nil
}

// Append 追加一条消息到存储。
func (s *Storage) Append(record Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. 写入日志文件，获取文件位置
	pos, size, err := s.logWriter.Write(record)
	if err != nil {
		return fmt.Errorf("write log failed: %w", err)
	}

	// 2. 写入 LevelDB 索引
	if err := s.index.Put(record.Topic, record.Partition, record.Offset, pos, size); err != nil {
		return fmt.Errorf("write index failed: %w", err)
	}

	// 3. 更新最大 offset
	if err := s.index.UpdateMaxOffset(record.Topic, record.Partition, record.Offset); err != nil {
		return fmt.Errorf("update max offset failed: %w", err)
	}

	return nil
}

// Read 从指定 offset 开始批量读取消息。
func (s *Storage) Read(topic string, partition int, offset int64, maxBatch int) ([]Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var records []Record
	for i := 0; i < maxBatch; i++ {
		curOffset := offset + int64(i)
		pos, size, err := s.index.Get(topic, partition, curOffset)
		if err != nil {
			// offset 不存在，停止读取
			break
		}

		record, err := s.logWriter.ReadAt(pos, size)
		if err != nil {
			return nil, fmt.Errorf("read log at %d failed: %w", pos, err)
		}

		records = append(records, record)
	}
	return records, nil
}

// MaxOffset 获取指定 topic-partition 的当前最大 offset。
func (s *Storage) MaxOffset(topic string, partition int) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index.GetMaxOffset(topic, partition)
}

// Close 关闭存储引擎。
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.logWriter.Close(); err != nil {
		return err
	}
	return s.db.Close()
}
