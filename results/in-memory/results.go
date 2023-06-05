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

func (r *Results) Get(ctx context.Context, uuid string) ([]byte, error) {
	r.mu.Lock()
	v, ok := r.store[uuid]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("value not found")
	}

	return v, nil
}

func (r *Results) DeleteJob(ctx context.Context, uuid string) error {
	r.mu.Lock()
	delete(r.store, uuid)
	delete(r.failed, uuid)
	delete(r.success, uuid)
	r.mu.Unlock()

	return nil
}

func (r *Results) Set(ctx context.Context, uuid string, b []byte) error {
	r.mu.Lock()
	r.store[uuid] = b
	r.mu.Unlock()

	return nil
}

func (r *Results) SetSuccess(_ context.Context, uuid string) error {
	r.mu.Lock()
	r.success[uuid] = struct{}{}
	r.mu.Unlock()

	return nil
}

func (r *Results) SetFailed(_ context.Context, uuid string) error {
	r.mu.Lock()
	r.failed[uuid] = struct{}{}
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
