package tasqueue

import (
	"fmt"

	"github.com/google/uuid"
)

var (
	defaultQueue = "tasqueue:tasks"
)

// Task represents a unit of work pushed by producers.
// It is the responsibility of the handler to unmarshal the payload and process it in any manner.
type Task struct {
	Queue   string
	Handler string
	Payload []byte
}

// TaskMessage is a wrapper over Task, used to transport the task over a broker.
// It contains additional fields such as status and a UUID.
type TaskMessage struct {
	UUID   string
	Status string
	Task   *Task
}

// NewTask returns a new unit of task with arbitrary payload. It accepts a list of options.
func NewTask(handler string, payload []byte, opts ...Opts) (*Task, error) {
	queue := defaultQueue

	for _, v := range opts {
		switch v.Name() {
		case customQueueOpt:
			queue = v.Value().(string)
		default:
			return nil, fmt.Errorf("invalid option %s for task", v.Name())
		}
	}

	return &Task{
		Queue:   queue,
		Handler: handler,
		Payload: payload,
	}, nil
}

// message() converts a task into a TaskMessage, ready to be enqueued onto the broker.
func (t *Task) message() *TaskMessage {
	return &TaskMessage{
		UUID:   uuid.NewString(),
		Task:   t,
		Status: statusStarted,
	}
}
