package inmemory

import (
	"context"
	"fmt"
	"sync"
)

type Results struct {
	mu    sync.Mutex
	store map[string][]byte
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
