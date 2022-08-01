package inmemory

import (
	"context"
	"fmt"
	"sync"
)

type Results struct {
	mu      sync.Mutex
	store   map[string][]byte
	failed  []string
	success []string
}

func New() *Results {
	return &Results{
		store: make(map[string][]byte),
	}
}

func (r *Results) Get(ctx context.Context, uuid string) ([]byte, error) {
	r.mu.Lock()
	v, ok := r.store[uuid]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("value not found")
	}

	return v, nil
}

func (r *Results) Set(ctx context.Context, uuid string, b []byte) error {
	r.mu.Lock()
	r.store[uuid] = b
	r.mu.Unlock()

	return nil
}

func (r *Results) SetSuccess(_ context.Context, uuid string) error {
	r.mu.Lock()
	r.success = append(r.success, uuid)
	r.mu.Unlock()

	return nil
}

func (r *Results) SetFailed(_ context.Context, uuid string) error {
	r.mu.Lock()
	r.failed = append(r.success, uuid)
	r.mu.Unlock()

	return nil
}

func (r *Results) GetSuccess(_ context.Context) ([]string, error) {
	r.mu.Lock()
	succ := r.success
	r.mu.Unlock()

	return succ, nil
}

func (r *Results) GetFailed(_ context.Context) ([]string, error) {
	r.mu.Lock()
	fail := r.failed
	r.mu.Unlock()

	return fail, nil
}
