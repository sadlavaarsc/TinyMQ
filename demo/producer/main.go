// demo/producer 是 TinyMQ 的生产者示例程序。
// 演示如何使用 Producer 客户端 SDK 向 Broker 发送消息。
package main

import (
	"fmt"
	"time"

	"TinyMQ/api"
)

func main() {
	// 创建生产者客户端，连接到 Broker HTTP 服务
	p := api.NewProducer("http://localhost:8080")

	// 循环发送 10 条消息
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("hello-%d-%s", i, time.Now().Format(time.RFC3339))
		if err := p.Send("test-topic", []byte(msg)); err != nil {
			fmt.Println("send error:", err)
		} else {
			fmt.Println("sent:", msg)
		}
		// 发送间隔，避免过快
		time.Sleep(500 * time.Millisecond)
	}

	fmt.Println("producer demo finished")
}
