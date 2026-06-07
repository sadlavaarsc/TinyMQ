// Package storage 实现 TinyMQ 的底层存储引擎。
package storage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// LogWriter 管理顺序日志文件的写入与随机读取。
type LogWriter struct {
	mu sync.RWMutex

	// file 是当前日志文件句柄
	file *os.File

	// writer 是带缓冲的写入器
	writer *bufio.Writer

	// currentPos 是当前文件写入位置
	currentPos int64

	// logDir 是日志文件目录
	logDir string
}

// NewLogWriter 创建 LogWriter，打开当前活跃日志文件。
func NewLogWriter(logDir string) (*LogWriter, error) {
	logPath := filepath.Join(logDir, "current.log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open log file failed: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat log file failed: %w", err)
	}

	return &LogWriter{
		file:       f,
		writer:     bufio.NewWriter(f),
		currentPos: stat.Size(),
		logDir:     logDir,
	}, nil
}

// Write 将 Record 写入日志文件，返回写入位置和大小。
func (lw *LogWriter) Write(record Record) (int64, int32, error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	// 序列化：| topicLen(2) | topic | partition(4) | offset(8) | timestamp(8) | payloadLen(4) | payload |
	topicBytes := []byte(record.Topic)
	topicLen := len(topicBytes)
	payloadLen := len(record.Payload)

	size := 2 + topicLen + 4 + 8 + 8 + 4 + payloadLen
	buf := make([]byte, 0, size)

	buf = append(buf, byte(topicLen>>8), byte(topicLen))
	buf = append(buf, topicBytes...)
	buf = append(buf, byte(record.Partition>>24), byte(record.Partition>>16), byte(record.Partition>>8), byte(record.Partition))
	buf = append(buf, int64ToBytes(record.Offset)...)
	buf = append(buf, int64ToBytes(record.Timestamp)...)
	buf = append(buf, byte(payloadLen>>24), byte(payloadLen>>16), byte(payloadLen>>8), byte(payloadLen))
	buf = append(buf, record.Payload...)

	pos := lw.currentPos
	if _, err := lw.writer.Write(buf); err != nil {
		return 0, 0, fmt.Errorf("write buffer failed: %w", err)
	}
	if err := lw.writer.Flush(); err != nil {
		return 0, 0, fmt.Errorf("flush failed: %w", err)
	}

	lw.currentPos += int64(size)
	return pos, int32(size), nil
}

// ReadAt 从指定位置读取指定大小的 Record。
func (lw *LogWriter) ReadAt(pos int64, size int32) (Record, error) {
	lw.mu.RLock()
	defer lw.mu.RUnlock()

	buf := make([]byte, size)
	if _, err := lw.file.ReadAt(buf, pos); err != nil {
		return Record{}, fmt.Errorf("read at %d failed: %w", pos, err)
	}

	return deserializeRecord(buf)
}

// Close 关闭日志文件。
func (lw *LogWriter) Close() error {
	lw.mu.Lock()
	defer lw.mu.Unlock()

	if err := lw.writer.Flush(); err != nil {
		return err
	}
	return lw.file.Close()
}

// int64ToBytes 将 int64 转为大端 8 字节。
func int64ToBytes(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// bytesToInt64 将大端 8 字节转为 int64.
func bytesToInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

// deserializeRecord 反序列化 Record。
func deserializeRecord(buf []byte) (Record, error) {
	if len(buf) < 26 {
		return Record{}, fmt.Errorf("buffer too short")
	}

	offset := 0
	topicLen := int(buf[offset])<<8 | int(buf[offset+1])
	offset += 2

	topic := string(buf[offset : offset+topicLen])
	offset += topicLen

	partition := int(buf[offset])<<24 | int(buf[offset+1])<<16 | int(buf[offset+2])<<8 | int(buf[offset+3])
	offset += 4

	recordOffset := bytesToInt64(buf[offset:])
	offset += 8

	timestamp := bytesToInt64(buf[offset:])
	offset += 8

	payloadLen := int(buf[offset])<<24 | int(buf[offset+1])<<16 | int(buf[offset+2])<<8 | int(buf[offset+3])
	offset += 4

	payload := buf[offset : offset+payloadLen]

	return Record{
		Topic:     topic,
		Partition: partition,
		Offset:    recordOffset,
		Timestamp: timestamp,
		Payload:   payload,
	}, nil
}
