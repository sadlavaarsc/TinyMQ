package main

import (
	"fmt"
	"github.com/sadlavaarsc/TinyMQ/api"
)

func main() {
	c := api.NewConsumer("http://localhost:9000", "group-1", "test-topic")
	for {
		msgs, err := c.Pull(5)
		if err != nil {
			fmt.Println("pull error:", err)
			continue
		}
		for _, msg := range msgs {
			fmt.Printf("received: topic=%s payload=%s\n", msg.Topic, string(msg.Payload))
			_ = c.Ack(msg.Offset)
		}
	}
}
