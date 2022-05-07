package tasqueue

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

const (
	DefaultQueue           = "tasqueue:tasks"
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
	Handler   string
	Payload   []byte

	queue    string
	maxRetry uint32
	schedule string
}

// scheduledTask holds the broker & results interfaces required to enqeueue a task.
// It has a Run() method that enqeues the task. This method is called by the cron scheduler.
type scheduledTask struct {
	log     *logrus.Logger
	ctx     context.Context
	broker  Broker
	results Results
	task    *Task
}

// newScheduled returns a scheduledTask used by the cron scheduler.
func newScheduled(ctx context.Context, log *logrus.Logger, b Broker, r Results, t *Task) *scheduledTask {
	return &scheduledTask{
		log:     log,
		ctx:     ctx,
		broker:  b,
		results: r,
		task:    t,
	}
}

// Run() lets scheduledTask implement the cron job interface.
// It uses the embedded broker and results interfaces to enqueue a task.
// Functionally, this method is similar to server.AddTask().
func (s *scheduledTask) Run() {
	// Set task status in the results backend.
	var (
		buff    bytes.Buffer
		encoder = gob.NewEncoder(&buff)
		msg     = s.task.message()
	)
	msg.setProcessedNow()
	msg.Status = statusStarted

	b, err := json.Marshal(msg)
	if err != nil {
		s.log.Error("could not marshal task message", err)
		return
	}
	if err := s.results.Set(s.ctx, msg.UUID, b); err != nil {
		s.log.Error("could not set task status", err)
		return
	}
	if err := encoder.Encode(msg); err != nil {
		s.log.Error("could not encode task message", err)
		return
	}
	if err := s.broker.Enqueue(s.ctx, buff.Bytes(), s.task.queue); err != nil {
		s.log.Error("could not enqueue task message", err)
	}

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
		queue           = DefaultQueue
		maxRetry uint32 = defaultMaxRetry
		schedule string
	)

	for _, v := range opts {
		switch v.Name() {
		case customQueueOpt:
			queue = v.Value().(string)
		case customMaxRetry:
			maxRetry = v.Value().(uint32)
		case customSchedule:
			schedule = v.Value().(string)
		default:
			return nil, fmt.Errorf("invalid option %s for task", v.Name())
		}
	}

	return &Task{
		queue:    queue,
		schedule: schedule,
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
