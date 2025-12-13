package tasqueue

import (
	"context"
	"time"
)

type Results interface {
	Get(ctx context.Context, id string) ([]byte, error)
	// NilError is used to check internally if the "id" is missing
	NilError() error
	Set(ctx context.Context, id string, b []byte) error
	// DeleteJob removes the job's saved metadata from the store
	DeleteJob(ctx context.Context, id string) error
	GetFailed(ctx context.Context) ([]string, error)
	GetSuccess(ctx context.Context) ([]string, error)
	SetFailed(ctx context.Context, id string) error
	SetSuccess(ctx context.Context, id string) error

	// Task management methods for external UI access
	SetTask(ctx context.Context, name string, task []byte) error
	GetTask(ctx context.Context, name string) ([]byte, error)
	GetAllTasks(ctx context.Context) ([][]byte, error)
	DeleteTask(ctx context.Context, name string) error
}

type Broker interface {
	// Enqueue places a task in the queue
	Enqueue(ctx context.Context, msg []byte, queue string) error

	// EnqueueScheduled accepts a task (msg, queue) and also a timestamp
	// The job should be enqueued at the particular timestamp.
	EnqueueScheduled(ctx context.Context, msg []byte, queue string, ts time.Time) error

	// Consume listens for tasks on the queue and calls processor
	Consume(ctx context.Context, work chan []byte, queue string)

	// GetPending returns a list of stored job messages on the particular queue
	// Deprecated: Use GetPendingWithPagination for better performance with large queues
	GetPending(ctx context.Context, queue string) ([]string, error)

	// GetPendingWithPagination returns a paginated list of stored job messages on the particular queue
	// offset: the starting index (0-based)
	// limit: maximum number of items to return
	// Returns: job messages, total count, error
	GetPendingWithPagination(ctx context.Context, queue string, offset, limit int) ([]string, int64, error)

	// GetPendingCount returns the count of pending jobs in the queue without fetching the actual jobs
	GetPendingCount(ctx context.Context, queue string) (int64, error)
}
