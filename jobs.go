package tasqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
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
	OnSuccess *Job
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
	UUID          string
	OnSuccessUUID string
	Status        string
	Queue         string
	Schedule      string
	MaxRetry      uint32
	Retried       uint32
	PrevErr       string
	ProcessedAt   time.Time
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

// Enqueue() accepts a job and returns the assigned UUID.
// The following steps take place:
// 1. Converts it into a job message, which assigns a UUID (among other meta info) to the job.
// 2. Sets the job status as "started" on the results store.
// 3. Enqueues the job (if the job is scheduled, pushes it onto the scheduler)
func (s *Server) Enqueue(ctx context.Context, t Job) (string, error) {
	var (
		msg = t.message()
	)

	// Set job status in the results backend.
	if err := s.statusStarted(ctx, msg); err != nil {
		return "", err
	}

	// If a schedule is set, add a cron job.
	if t.opts.schedule != "" {
		if err := s.enqueueScheduled(ctx, msg); err != nil {
			return "", err
		}
		return msg.UUID, nil
	}

	if err := s.enqueue(ctx, msg); err != nil {
		return "", err
	}

	return msg.UUID, nil
}

func (s *Server) enqueueScheduled(ctx context.Context, msg JobMessage) error {
	schJob := newScheduled(ctx, s.log, s.broker, msg)
	// TODO: maintain a map of scheduled cron tasks
	if _, err := s.cron.AddJob(msg.Schedule, schJob); err != nil {
		return err
	}

	return nil
}

func (s *Server) enqueue(ctx context.Context, msg JobMessage) error {
	b, err := msgpack.Marshal(msg)
	if err != nil {
		return err
	}

	if err := s.broker.Enqueue(ctx, b, msg.Queue); err != nil {
		return err
	}

	return nil
}

func (s *Server) setJobMessage(ctx context.Context, t JobMessage) error {
	b, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("could not set job message in store : %w", err)
	}
	if err := s.results.Set(ctx, t.UUID, b); err != nil {
		return fmt.Errorf("could not set job message in store : %w", err)
	}

	return nil
}

// GetJob() accepts a UUID and returns the job message in the results store.
// This is useful to check the status of a job message.
func (s *Server) GetJob(ctx context.Context, uuid string) (JobMessage, error) {
	s.log.Infof("getting job : %s", uuid)

	b, err := s.results.Get(ctx, uuid)
	if err != nil {
		return JobMessage{}, err
	}

	var t JobMessage
	if err := json.Unmarshal(b, &t); err != nil {
		return JobMessage{}, err
	}

	return t, nil
}
