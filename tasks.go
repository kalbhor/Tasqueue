package tasqueue

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	resultsSuffix          = ":result"
	DefaultQueue           = "tasqueue:tasks"
	defaultMaxRetry uint32 = 3
)

// Job represents a unit of work pushed by producers.
// It is the responsibility of the task handler to unmarshal (if required) the payload and process it in any manner.
type Job struct {
	// If task is successful, the OnSuccess jobs are enqueued.
	OnSuccess []*Job
	Handler   string
	Payload   []byte

	opts jobOpts
}

// jobOpts holds the various options available to configure a job.
type jobOpts struct {
	queue    string
	maxRetry uint32
	schedule string
}

// NewJob returns a new unit of task with arbitrary payload. It accepts a list of options.
func NewJob(handler string, payload []byte, opts ...Opts) (*Job, error) {

	// Create the job options with default values.
	var jOpts = jobOpts{queue: DefaultQueue, maxRetry: defaultMaxRetry}

	for _, v := range opts {
		switch v.Name() {
		case customQueueOpt:
			jOpts.queue = v.Value().(string)
		case customMaxRetry:
			jOpts.maxRetry = v.Value().(uint32)
		case customSchedule:
			jOpts.schedule = v.Value().(string)
		default:
			return nil, fmt.Errorf("invalid option %s for task", v.Name())
		}
	}

	return &Job{
		opts:    jOpts,
		Handler: handler,
		Payload: payload,
	}, nil
}

// NewChain() accepts a list of Tasks and creates a chain by setting the
// onSuccess task of i'th task to (i+1)'th task, hence forming a "chain".
// It returns the first task (essentially the first node of the linked list), which can be queued normally.
func NewChain(tasks ...*Job) (*Job, error) {
	if len(tasks) < 2 {
		return nil, fmt.Errorf("minimum 2 tasks required to form chain")
	}

	// Set the on success tasks as the i+1 task,
	// hence forming a "chain" of tasks.
	for i := 0; i < len(tasks)-1; i++ {
		tasks[i].OnSuccess = []*Job{tasks[i+1]}
	}

	return tasks[0], nil
}

// Meta contains fields related to a job. These are updated when a task is consumed.
type Meta struct {
	UUID        string
	Status      string
	MaxRetry    uint32
	Retried     uint32
	PrevErr     string
	ProcessedAt time.Time
}

// JobCtx is passed onto handler functions.
type JobCtx struct {
	store Results
	Meta  Meta
}

// Save() sets arbitrary results for a job on the results store.
func (c *JobCtx) Save(b []byte) error {
	return c.store.Set(nil, c.Meta.UUID+resultsSuffix, b)
}

// JobMessage is a wrapper over Task, used to transport the task over a broker.
// It contains additional fields such as status and a UUID.
type JobMessage struct {
	Meta
	Job *Job
}

// message() converts a task into a TaskMessage, ready to be enqueued onto the broker.
func (t *Job) message() JobMessage {
	return JobMessage{
		Meta: Meta{
			UUID:     uuid.NewString(),
			Status:   statusStarted,
			MaxRetry: t.opts.maxRetry,
		},
		Job: t,
	}
}

// setError sets the task message's error.
func (t *JobMessage) setError(err error) {
	t.PrevErr = err.Error()
}

// setProcessedNow() sets the message's processedAt time as now().
func (t *JobMessage) setProcessedNow() {
	t.ProcessedAt = time.Now()
}
