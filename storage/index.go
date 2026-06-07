// Package storage 实现 TinyMQ 的底层存储引擎。
package storage

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
)

// Index 基于 LevelDB 实现消息索引，维护 offset -> 文件位置的映射。
type Index struct {
	db *leveldb.DB
}

// NewIndex 创建 Index 实例。
func NewIndex(db *leveldb.DB) *Index {
	return &Index{db: db}
}

// buildKey 构造索引 key：topic|partition|offset。
func buildKey(topic string, partition int, offset int64) []byte {
	return []byte(fmt.Sprintf("%s|%d|%d", topic, partition, offset))
}

// buildMaxOffsetKey 构造最大 offset 的 key：maxoffset|topic|partition。
func buildMaxOffsetKey(topic string, partition int) []byte {
	return []byte(fmt.Sprintf("maxoffset|%s|%d", topic, partition))
}

// Put 写入一条索引记录。
func (idx *Index) Put(topic string, partition int, offset int64, pos int64, size int32) error {
	key := buildKey(topic, partition, offset)
	value := fmt.Sprintf("%d|%d", pos, size)
	return idx.db.Put(key, []byte(value), nil)
}

// Get 查询指定 offset 的文件位置与大小。
func (idx *Index) Get(topic string, partition int, offset int64) (int64, int32, error) {
	key := buildKey(topic, partition, offset)
	data, err := idx.db.Get(key, nil)
	if err != nil {
		return 0, 0, err
	}

	parts := strings.Split(string(data), "|")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid index value: %s", string(data))
	}

	pos, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	size, err := strconv.ParseInt(parts[1], 10, 32)
	if err != nil {
		return 0, 0, err
	}
	return pos, int32(size), nil
}

// UpdateMaxOffset 更新 topic-partition 的最大 offset。
func (idx *Index) UpdateMaxOffset(topic string, partition int, offset int64) error {
	key := buildMaxOffsetKey(topic, partition)
	cur, err := idx.db.Get(key, nil)
	if err != nil && err != leveldb.ErrNotFound {
		return err
	}

	var curOffset int64 = -1
	if err == nil {
		curOffset, _ = strconv.ParseInt(string(cur), 10, 64)
	}

	if offset > curOffset {
		return idx.db.Put(key, []byte(strconv.FormatInt(offset, 10)), nil)
	}
	return nil
}

// GetMaxOffset 获取 topic-partition 的当前最大 offset。
func (idx *Index) GetMaxOffset(topic string, partition int) (int64, error) {
	key := buildMaxOffsetKey(topic, partition)
	data, err := idx.db.Get(key, nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return -1, nil
		}
		return 0, err
	}
	offset, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return 0, err
	}
	return offset, nil
}
