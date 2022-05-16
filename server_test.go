package tasqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
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

func TestEnqueue(t *testing.T) {
	srv, err := NewServer(NewMockBroker(), NewMockResults())
	if err != nil {
		t.Fatal(err)
	}
	srv.RegisterTask("mock_handler", MockHandler)

	j, err := json.Marshal(MockPayload{ShouldErr: false})
	if err != nil {
		t.Fatal(err)
	}

	job, err := NewJob("mock_handler", j)
	if err != nil {
		t.Fatal(err)
	}

	uuid, err := srv.Enqueue(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Enqueued job with uuid : %s\n", uuid)
}

func TestEnqueueGroup(t *testing.T) {
	srv, err := NewServer(NewMockBroker(), NewMockResults())
	if err != nil {
		t.Fatal(err)
	}
	srv.RegisterTask("mock_handler", MockHandler)

	j, err := json.Marshal(MockPayload{ShouldErr: false})
	if err != nil {
		t.Fatal(err)
	}

	job1, err := NewJob("mock_handler", j)
	if err != nil {
		t.Fatal(err)
	}
	job2, err := NewJob("mock_handler", j)
	if err != nil {
		t.Fatal(err)
	}

	group, err := NewGroup(job1, job2)
	if err != nil {
		t.Fatal(err)
	}

	uuid, err := srv.EnqueueGroup(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Enqueued job with uuid : %s\n", uuid)
}

func TestExecJob(t *testing.T) {
	jobs := []MockPayload{
		{ShouldErr: false},
		{ShouldErr: false},
	}
	srv, err := NewServer(NewMockBroker(), NewMockResults())
	if err != nil {
		t.Fatal(err)
	}
	srv.RegisterTask("mock_handler", MockHandler)
	go srv.Start(context.Background())

	for _, job := range jobs {
		j, err := json.Marshal(job)
		if err != nil {
			t.Fatal(err)
		}

		pl, err := NewJob("mock_handler", j)
		if err != nil {
			t.Fatal(err)
		}

		uuid, err := srv.Enqueue(context.Background(), pl)
		if err != nil {
			t.Fatal(err)
		}

		msg, err := srv.GetJob(context.Background(), uuid)
		if err != nil {
			t.Fatal(err)
		}

		if err := srv.execJob(context.Background(), msg, MockHandler); err != nil {
			t.Fatal(err)
		}

		msg, err = srv.GetJob(context.Background(), uuid)
		if err != nil {
			t.Fatal(err)
		}

		switch job.ShouldErr {
		case false:
			if msg.Status != statusDone {
				t.Fatalf("incorrect job status, expected %s, got %s", statusDone, msg.Status)
			}
		case true:
			if msg.Status != statusFailed {
				t.Fatalf("incorrect job status, expected %s, got %s", statusFailed, msg.Status)
			}
		}
	}
}
