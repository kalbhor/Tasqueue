package tasqueue

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	resultsPrefix          = "tasqueue:result:"
	DefaultQueue           = "tasqueue:tasks"
	defaultMaxRetry uint32 = 1
)

// Job represents a unit of work pushed by producers.
// It is the responsibility of the task handler to unmarshal (if required) the payload and process it in any manner.
type Job struct {
	// If task is successful, the OnSuccess jobs are enqueued.
	OnSuccess []Job
	Task      string
	Payload   []byte

	opts jobOpts
}

// jobOpts holds the various options available to configure a job.
type jobOpts struct {
	queue    string
	maxRetry uint32
	schedule string
}

// Meta contains fields related to a job. These are updated when a task is consumed.
type Meta struct {
	UUID        string
	Status      string
	Queue       string
	Schedule    string
	MaxRetry    uint32
	Retried     uint32
	PrevErr     string
	ProcessedAt time.Time
}

// NewJob returns a job with arbitrary payload.
// It accepts the name of the task, the payload and a list of options.
func NewJob(handler string, payload []byte, opts ...Opts) (Job, error) {

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
			return Job{}, fmt.Errorf("invalid option %s for task", v.Name())
		}
	}

	return Job{
		opts:    jOpts,
		Task:    handler,
		Payload: payload,
	}, nil
}

// JobCtx is passed onto handler functions. It allows access to a job's meta information to the handler.
type JobCtx struct {
	store Results
	Meta  Meta
}

// Save() sets arbitrary results for a job on the results store.
func (c *JobCtx) Save(b []byte) error {
	return c.store.Set(nil, resultsPrefix+c.Meta.UUID, b)
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
			Status:   StatusStarted,
			MaxRetry: t.opts.maxRetry,
			Schedule: t.opts.schedule,
			Queue:    t.opts.queue,
		},
		Job: t,
	}
}

type Group struct {
	Jobs []Job
}

// GroupMeta contains fields related to a group job. These are updated when a task is consumed.
type GroupMeta struct {
	UUID   string
	Status string
	// JobStatus is a map of job uuid -> status
	JobStatus map[string]string
}

// GroupMessage is a wrapper over Group, containing meta info such as status, uuid.
// A GroupMessage is stored in the results store.
type GroupMessage struct {
	GroupMeta
	Group *Group
}

// message() converts a group into a group message, ready to be enqueued/stored.
func (t *Group) message() GroupMessage {
	return GroupMessage{
		GroupMeta: GroupMeta{
			JobStatus: make(map[string]string),
			UUID:      uuid.NewString(),
			Status:    StatusProcessing,
		},
		Group: t,
	}
}

// NewGroup() accepts a list of jobs and creates a group.
func NewGroup(j ...Job) (Group, error) {
	if len(j) < 2 {
		return Group{}, fmt.Errorf("minimum 2 tasks required to form group")
	}

	return Group{
		Jobs: j,
	}, nil

}

// NewChain() accepts a list of Tasks and creates a chain by setting the
// onSuccess task of i'th task to (i+1)'th task, hence forming a "chain".
// It returns the first task (essentially the first node of the linked list), which can be queued normally.
func NewChain(j ...Job) (Job, error) {
	if len(j) < 2 {
		return Job{}, fmt.Errorf("minimum 2 tasks required to form chain")
	}

	// Set the on success tasks as the i+1 task,
	// hence forming a "chain" of tasks.
	for i := 0; i < len(j)-1; i++ {
		j[i].OnSuccess = []Job{j[i+1]}
	}

	return j[0], nil
}
