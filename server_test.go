package tasqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/zerodha/logf"
)

const (
	taskName = "mock_handler"
)

func newServer(t *testing.T) *Server {
	lo := logf.New(logf.Opts{
		Level: logf.DebugLevel,
	})
	srv, err := NewServer(NewMockBroker(), NewMockResults(), lo)
	if err != nil {
		t.Fatal(err)
	}
	srv.RegisterTask(taskName, MockHandler, TaskOpts{
		Concurrency: 5,
	})

	return srv
}

type MockPayload struct {
	ShouldErr bool
}

func MockHandler(msg []byte, _ JobCtx) error {
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

func (r *MockResults) Get(_ context.Context, uuid string) ([]byte, error) {
	r.mu.Lock()
	v, ok := r.store[uuid]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("value not found")
	}

	return v, nil
}

func (r *MockResults) Set(_ context.Context, uuid string, b []byte) error {
	r.mu.Lock()
	r.store[uuid] = b
	r.mu.Unlock()

	return nil
}

type MockBroker struct {
	queues map[string][][]byte
	data   chan []byte
}

func NewMockBroker() *MockBroker {
	return &MockBroker{
		queues: make(map[string][][]byte),
		data:   make(chan []byte, 100),
	}
}

func (r *MockBroker) Consume(ctx context.Context, work chan []byte, _ string) {
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

func (r *MockBroker) Enqueue(_ context.Context, msg []byte, _ string) error {
	r.data <- msg
	return nil
}
