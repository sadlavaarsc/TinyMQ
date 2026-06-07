package main

import (
	"fmt"
	"github.com/sadlavaarsc/TinyMQ/api"
)

func main() {
	p := api.NewProducer("http://localhost:9000")
	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("hello-%d", i)
		if err := p.Send("test-topic", []byte(msg)); err != nil {
			fmt.Println("send error:", err)
		} else {
			fmt.Println("sent:", msg)
		}
	}
}
