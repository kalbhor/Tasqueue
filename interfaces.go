package tasqueue

import (
	"context"
	"time"
)

type Results interface {
	Get(ctx context.Context, id string) ([]byte, error)
	Set(ctx context.Context, id string, b []byte) error
	// DeleteJob removes the job's saved metadata from the store
	DeleteJob(ctx context.Context, id string) error
	GetFailed(ctx context.Context) ([]string, error)
	GetSuccess(ctx context.Context) ([]string, error)
	SetFailed(ctx context.Context, id string) error
	SetSuccess(ctx context.Context, id string) error
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
	GetPending(ctx context.Context, queue string) ([]string, error)
}
