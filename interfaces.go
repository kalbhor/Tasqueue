package tasqueue

import "context"

type Results interface {
	Get(ctx context.Context, uuid string) ([]byte, error)
	Set(ctx context.Context, uuid string, b []byte) error
}

type Broker interface {
	// Enqueue places a task in the queue
	Enqueue(ctx context.Context, msg []byte, queue string) error

	// Consume listens for tasks on the queue and calls processor
	Consume(ctx context.Context, work chan []byte, queue string)
}

// Opts is an interface to define arbitratry options.
type Opts interface {
	Name() string
	Value() interface{}
}
