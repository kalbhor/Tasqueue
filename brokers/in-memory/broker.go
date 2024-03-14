package inmemory

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Broker struct {
	mu     sync.RWMutex
	queues map[string]chan []byte

	pmu     sync.RWMutex
	pending map[string][]string
}

func New() *Broker {
	return &Broker{
		queues:  make(map[string]chan []byte),
		pending: make(map[string][]string),
	}
}

func (r *Broker) Consume(ctx context.Context, work chan []byte, queue string) {
	r.mu.RLock()
	ch, ok := r.queues[queue]
	r.mu.RUnlock()

	// If the queue isn't found, make a queue.
	if !ok {
		ch = make(chan []byte, 100)
		r.mu.Lock()
		r.queues[queue] = ch
		r.mu.Unlock()

	}

	for {
		select {
		case <-ctx.Done():
			fmt.Println("stopping consumer")
			return
		case d := <-ch:
			r.pmu.Lock()
			r.pending[queue] = r.pending[queue][1:]
			r.pmu.Unlock()

			r.mu.RLock()
			work <- d
			r.mu.RUnlock()
		}
	}
}

func (r *Broker) Enqueue(ctx context.Context, msg []byte, queue string) error {
	r.mu.RLock()
	ch, ok := r.queues[queue]
	r.mu.RUnlock()

	if !ok {
		ch = make(chan []byte, 100)
		r.mu.Lock()
		r.queues[queue] = ch
		r.mu.Unlock()

	}

	r.pmu.Lock()
	r.pending[queue] = append(r.pending[queue], string(msg))
	r.pmu.Unlock()

	ch <- msg
	return nil
}

func (r *Broker) GetPending(ctx context.Context, queue string) ([]string, error) {
	r.pmu.Lock()
	pending, ok := r.pending[queue]
	r.pmu.Unlock()
	if !ok {
		return nil, fmt.Errorf("non existend queue provided")
	}

	return pending, nil
}

func (b *Broker) EnqueueScheduled(ctx context.Context, msg []byte, queue string, ts time.Time) error {
	return fmt.Errorf("in-memory broker does not support this method")
}
