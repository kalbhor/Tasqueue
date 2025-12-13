package inmemory

import (
	"context"
	"errors"
	"sync"
)

type Results struct {
	mu      sync.Mutex
	store   map[string][]byte
	failed  map[string]struct{}
	success map[string]struct{}
	tasks   map[string][]byte
}

func New() *Results {
	return &Results{
		store:   make(map[string][]byte),
		failed:  make(map[string]struct{}),
		success: make(map[string]struct{}),
		tasks:   make(map[string][]byte),
	}
}

var errNotFound = errors.New("could not find result in in-memory store")

func (r *Results) Get(ctx context.Context, id string) ([]byte, error) {
	r.mu.Lock()
	v, ok := r.store[id]
	r.mu.Unlock()
	if !ok {
		return nil, errNotFound
	}

	return v, nil
}

func (r *Results) NilError() error {
	return errNotFound
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

// SetTask stores task metadata in memory
func (r *Results) SetTask(_ context.Context, name string, task []byte) error {
	r.mu.Lock()
	r.tasks[name] = task
	r.mu.Unlock()

	return nil
}

// GetTask retrieves task metadata from memory
func (r *Results) GetTask(_ context.Context, name string) ([]byte, error) {
	r.mu.Lock()
	task, ok := r.tasks[name]
	r.mu.Unlock()

	if !ok {
		return nil, errNotFound
	}

	return task, nil
}

// GetAllTasks retrieves all task metadata from memory
func (r *Results) GetAllTasks(_ context.Context) ([][]byte, error) {
	r.mu.Lock()
	tasks := make([][]byte, 0, len(r.tasks))
	for _, task := range r.tasks {
		tasks = append(tasks, task)
	}
	r.mu.Unlock()

	return tasks, nil
}

// DeleteTask removes task metadata from memory
func (r *Results) DeleteTask(_ context.Context, name string) error {
	r.mu.Lock()
	delete(r.tasks, name)
	r.mu.Unlock()

	return nil
}
