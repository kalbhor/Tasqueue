package inmemory

import (
	"context"
	"fmt"
	"sync"
)

type Broker struct {
	mu     sync.Mutex
	queues map[string][][]byte
	data   chan []byte
}

func New() *Broker {
	return &Broker{
		queues: make(map[string][][]byte),
		data:   make(chan []byte, 100),
	}
}

func (r *Broker) Consume(ctx context.Context, work chan []byte, queue string) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("stopping consumer")
			return
		case d := <-r.data:
			work <- d
		}
	}
}

func (r *Broker) Enqueue(ctx context.Context, msg []byte, queue string) error {
	r.data <- msg
	return nil
}
