// Package replication 实现 TinyMQ 的主从复制与故障切换。
package replication

import (
	"fmt"
	"sync"
	"time"
)

// FailoverManager 故障切换管理器
type FailoverManager struct {
	mu       sync.Mutex
	master   string
	slaves   []string
	healthy  map[string]bool
	interval time.Duration
}

// NewFailoverManager 创建故障切换管理器
func NewFailoverManager(master string, slaves []string) *FailoverManager {
	f := &FailoverManager{
		master:   master,
		slaves:   slaves,
		healthy:  make(map[string]bool),
		interval: 3 * time.Second,
	}
	for _, s := range slaves {
		f.healthy[s] = true
	}
	return f
}

// StartMonitoring 启动健康监测
func (f *FailoverManager) StartMonitoring() {
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()
	for range ticker.C {
		f.checkHealth()
	}
}

func (f *FailoverManager) checkHealth() {
	f.mu.Lock()
	defer f.mu.Unlock()
	// 简化：模拟健康检查
	// 实际生产环境应发送心跳请求
	if !f.healthy[f.master] {
		// 主节点宕机，触发选举
		f.electNewMaster()
	}
}

func (f *FailoverManager) electNewMaster() {
	for _, s := range f.slaves {
		if f.healthy[s] {
			fmt.Println("[Failover] elect new master:", s)
			f.master = s
			return
		}
	}
	fmt.Println("[Failover] no available slave")
}

// ReportHealth 上报节点健康状态
func (f *FailoverManager) ReportHealth(addr string, healthy bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.healthy[addr] = healthy
}
