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
	// Ensure work channel is closed when producer (broker) is done
	defer close(work)
	
	r.mu.RLock()
	ch, ok := r.queues[queue]
	r.mu.RUnlock()

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

			work <- d
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

// GetPendingWithPagination returns a paginated list of pending jobs from the in-memory queue
func (r *Broker) GetPendingWithPagination(ctx context.Context, queue string, offset, limit int) ([]string, int64, error) {
	r.pmu.RLock()
	pending, ok := r.pending[queue]
	r.pmu.RUnlock()

	if !ok {
		// Queue doesn't exist yet, return empty result
		return []string{}, 0, nil
	}

	total := int64(len(pending))
	if total == 0 {
		return []string{}, 0, nil
	}

	// Validate offset
	if offset < 0 {
		offset = 0
	}
	if int64(offset) >= total {
		return []string{}, total, nil
	}

	// Validate limit
	if limit <= 0 {
		limit = 100 // Default limit
	}
	// Cap maximum limit to prevent abuse
	if limit > 10000 {
		limit = 10000
	}

	// Calculate end index
	end := offset + limit
	if int64(end) > total {
		end = int(total)
	}

	// Return slice of pending jobs
	result := make([]string, end-offset)
	copy(result, pending[offset:end])

	return result, total, nil
}

// GetPendingCount returns the count of pending jobs in the in-memory queue
func (r *Broker) GetPendingCount(ctx context.Context, queue string) (int64, error) {
	r.pmu.RLock()
	pending, ok := r.pending[queue]
	r.pmu.RUnlock()

	if !ok {
		// Queue doesn't exist yet, return 0 count
		return 0, nil
	}

	return int64(len(pending)), nil
}

func (b *Broker) EnqueueScheduled(ctx context.Context, msg []byte, queue string, ts time.Time) error {
	return fmt.Errorf("in-memory broker does not support this method")
}
