// demo/consumer 是 TinyMQ 的消费者示例程序。
// 演示如何使用 Consumer 客户端 SDK 加入 Consumer Group、拉取消息并提交 ACK。
package main

import (
	"fmt"
	"time"

	"TinyMQ/api"
)

func main() {
	// 创建消费者客户端，连接到 Broker HTTP 服务
	c := api.NewConsumer("http://localhost:8080", "group-1", "test-topic")

	// 持续拉取消息
	for {
		msgs, err := c.Pull(5)
		if err != nil {
			fmt.Println("pull error:", err)
			// 拉取失败时等待后重试
			time.Sleep(2 * time.Second)
			continue
		}

		if len(msgs) == 0 {
			fmt.Println("no new messages, waiting...")
			time.Sleep(1 * time.Second)
			continue
		}

		for _, msg := range msgs {
			fmt.Printf("received: topic=%s partition=%d offset=%d payload=%s\n",
				msg.Topic, msg.Partition, msg.Offset, string(msg.Payload))

			// 模拟业务处理
			time.Sleep(200 * time.Millisecond)

			// 提交 ACK，确认消息已消费
			if err := c.Ack(msg.Offset); err != nil {
				fmt.Println("ack error:", err)
			} else {
				fmt.Printf("acked: offset=%d\n", msg.Offset)
			}
		}
	}
}
