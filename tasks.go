package tasqueue

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

var (
	defaultQueue           = "tasqueue:tasks"
	defaultMaxRetry uint32 = 3
)

// NewChain() accepts a list of Tasks and creates a chain by setting the
// onSuccess task to a subsequent task hence forming a "chain".
func NewChain(tasks ...*Task) (*Task, error) {
	if len(tasks) < 2 {
		return nil, fmt.Errorf("minimum 2 tasks required to form chain")
	}

	// Set the on success tasks as the i+1 task,
	// hence forming a "chain" of tasks.
	for i := 0; i < len(tasks)-1; i++ {
		tasks[i].OnSuccess = []*Task{tasks[i+1]}
	}

	return tasks[0], nil
}

// Task represents a unit of work pushed by producers.
// It is the responsibility of the handler to unmarshal the payload and process it in any manner.
type Task struct {
	// If task is successful, the OnSuccess tasks are enqueued.
	OnSuccess []*Task
	Queue     string
	Handler   string
	Payload   []byte

	maxRetry uint32
}

// TaskMessage is a wrapper over Task, used to transport the task over a broker.
// It contains additional fields such as status and a UUID.
type TaskMessage struct {
	UUID        string
	Status      string
	MaxRetry    uint32
	Retried     uint32
	PrevErr     string
	ProcessedAt time.Time
	Task        *Task
}

// NewTask returns a new unit of task with arbitrary payload. It accepts a list of options.
func NewTask(handler string, payload []byte, opts ...Opts) (*Task, error) {
	var (
		queue           = defaultQueue
		maxRetry uint32 = defaultMaxRetry
	)

	for _, v := range opts {
		switch v.Name() {
		case customQueueOpt:
			queue = v.Value().(string)
		case customMaxRetry:
			maxRetry = v.Value().(uint32)
		default:
			return nil, fmt.Errorf("invalid option %s for task", v.Name())
		}
	}

	return &Task{
		Queue:    queue,
		Handler:  handler,
		Payload:  payload,
		maxRetry: maxRetry,
	}, nil
}

// message() converts a task into a TaskMessage, ready to be enqueued onto the broker.
func (t *Task) message() TaskMessage {
	return TaskMessage{
		UUID:     uuid.NewString(),
		Task:     t,
		Status:   statusStarted,
		MaxRetry: t.maxRetry,
	}
}

// setError sets the task message's error.
func (t *TaskMessage) setError(err error) {
	t.PrevErr = err.Error()
}

// setProcessedNow() sets the message's processedAt time as now().
func (t *TaskMessage) setProcessedNow() {
	t.ProcessedAt = time.Now()
}
