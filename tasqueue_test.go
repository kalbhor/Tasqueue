package tasqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

type MockPayload struct {
	ShouldErr bool
}

func MockHandler(msg []byte, ctx *JobCtx) error {
	var m MockPayload
	if err := json.Unmarshal(msg, &m); err != nil {
		return err
	}

	if m.ShouldErr {
		return fmt.Errorf("task ended with error")
	}

	return nil
}

type MockResults struct {
	mu    sync.Mutex
	store map[string][]byte
}

func NewMockResults() *MockResults {
	return &MockResults{
		store: make(map[string][]byte),
	}
}

func (r *MockResults) Get(ctx context.Context, uuid string) ([]byte, error) {
	r.mu.Lock()
	v, ok := r.store[uuid]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("value not found")
	}

	return v, nil
}

func (r *MockResults) Set(ctx context.Context, uuid string, b []byte) error {
	r.mu.Lock()
	r.store[uuid] = b
	r.mu.Unlock()

	return nil
}

type MockBroker struct {
	mu     sync.Mutex
	queues map[string][][]byte
}

func NewMockBroker() *MockBroker {
	return &MockBroker{
		queues: make(map[string][][]byte),
	}
}

func (r *MockBroker) Consume(ctx context.Context, work chan []byte, queue string) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("stopping consumer")
			return
		case <-time.Tick(time.Second * 3):
			r.mu.Lock()
			q := r.queues[queue]
			r.mu.Unlock()

			if len(q) > 0 {
				work <- q[len(q)-1]
				r.mu.Lock()
				r.queues[queue] = q[:len(q)-1]
				r.mu.Unlock()
			}
		}
	}
}

func (r *MockBroker) Enqueue(ctx context.Context, msg []byte, queue string) error {
	r.mu.Lock()
	q := r.queues[queue]
	q = append(q, msg)
	r.queues[queue] = q
	r.mu.Unlock()

	return nil
}

func successCB(j *JobCtx) {
	log.Println("success callback..")
}

func makeJob(f bool) *Job {
	j, _ := json.Marshal(&MockPayload{ShouldErr: f})
	job, _ := NewJob("mock_handler", j, CustomMaxRetry(0), SuccessCallback(successCB))
	return job
}

func makeGroup(fs ...bool) *Group {
	var jobs []*Job
	for _, f := range fs {
		jobs = append(jobs, makeJob(f))
	}
	grp, _ := NewGroup(jobs...)

	return grp
}
