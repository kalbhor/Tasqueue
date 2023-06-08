package inmemory

import (
	"context"
	"fmt"
	"sync"
)

type Results struct {
	mu      sync.Mutex
	store   map[string][]byte
	failed  map[string]struct{}
	success map[string]struct{}
}

func New() *Results {
	return &Results{
		store:   make(map[string][]byte),
		failed:  make(map[string]struct{}),
		success: make(map[string]struct{}),
	}
}

func (r *Results) Get(ctx context.Context, id string) ([]byte, error) {
	r.mu.Lock()
	v, ok := r.store[id]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("value not found")
	}

	return v, nil
}

func (r *Results) DeleteJob(ctx context.Context, id string) error {
	r.mu.Lock()
	delete(r.store, id)
	delete(r.failed, id)
	delete(r.success, id)
	r.mu.Unlock()

	return nil
}

func (r *Results) Set(ctx context.Context, id string, b []byte) error {
	r.mu.Lock()
	r.store[id] = b
	r.mu.Unlock()

	return nil
}

func (r *Results) SetSuccess(_ context.Context, id string) error {
	r.mu.Lock()
	r.success[id] = struct{}{}
	r.mu.Unlock()

	return nil
}

func (r *Results) SetFailed(_ context.Context, id string) error {
	r.mu.Lock()
	r.failed[id] = struct{}{}
	r.mu.Unlock()

	return nil
}

func (r *Results) GetSuccess(_ context.Context) ([]string, error) {
	r.mu.Lock()
	var succ = make([]string, len(r.success))
	for k := range r.success {
		succ = append(succ, k)
	}
	r.mu.Unlock()

	return succ, nil
}

func (r *Results) GetFailed(_ context.Context) ([]string, error) {
	r.mu.Lock()
	var fl = make([]string, len(r.failed))
	for k := range r.failed {
		fl = append(fl, k)
	}
	r.mu.Unlock()

	return fl, nil
}
