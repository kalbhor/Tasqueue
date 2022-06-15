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

	opts JobOpts
}

// JobOpts holds the various options available to configure a job.
type JobOpts struct {
	Queue      string
	MaxRetries uint32
	Schedule   string
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

	// PrevJobResults contains any job results set by a previous job in a chain.
	// This will be nil if the previous job doesn't set the results on JobCtx.
	PrevJobResults []byte
}

// DefaultMeta returns Meta with a UUID and other defaults filled in.
func DefaultMeta(opts JobOpts) Meta {
	return Meta{
		UUID:     uuid.NewString(),
		Status:   StatusStarted,
		MaxRetry: opts.MaxRetries,
		Schedule: opts.Schedule,
		Queue:    opts.Queue,
	}
}

// NewJob returns a job with arbitrary payload.
// It accepts the name of the task, the payload and a list of options.
func NewJob(handler string, payload []byte, opts JobOpts) (Job, error) {
	if opts.Queue == "" {
		opts.Queue = DefaultQueue
	}

	return Job{
		opts:    opts,
		Task:    handler,
		Payload: payload,
	}, nil
}

// JobCtx is passed onto handler functions. It allows access to a job's meta information to the handler.
type JobCtx struct {
	store Results
	// results just holds the results set by calling Save().
	results []byte
	Meta    Meta
}

// Save() sets arbitrary results for a job on the results store.
func (c *JobCtx) Save(b []byte) error {
	// TODO: Maybe recieve context in Save()
	// Store saved in the job ctx as well to easily get the result instead of
	// going to the broker every time.
	c.results = b
	return c.store.Set(context.Background(), resultsPrefix+c.Meta.UUID, b)
}

// JobMessage is a wrapper over Task, used to transport the task over a broker.
// It contains additional fields such as status and a UUID.
type JobMessage struct {
	Meta
	Job *Job
}

// message() converts a task into a TaskMessage, ready to be enqueued onto the broker.
func (t *Job) message(meta Meta) JobMessage {
	return JobMessage{
		Meta: meta,
		Job:  t,
	}
}

// Enqueue() accepts a job and returns the assigned UUID.
// The following steps take place:
// 1. Converts it into a job message, which assigns a UUID (among other meta info) to the job.
// 2. Sets the job status as "started" on the results store.
// 3. Enqueues the job (if the job is scheduled, pushes it onto the scheduler)
func (s *Server) Enqueue(ctx context.Context, t Job) (string, error) {
	return s.enqueueWithMeta(ctx, t, DefaultMeta(t.opts))
}

func (s *Server) enqueueWithMeta(ctx context.Context, t Job, meta Meta) (string, error) {
	var (
		msg = t.message(meta)
	)

	// Set job status in the results backend.
	if err := s.statusStarted(ctx, msg); err != nil {
		return "", err
	}

	// If a schedule is set, add a cron job.
	if t.opts.Schedule != "" {
		if err := s.enqueueScheduled(ctx, msg); err != nil {
			return "", err
		}
		return msg.UUID, nil
	}

	if err := s.enqueueMessage(ctx, msg); err != nil {
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

func (s *Server) enqueueMessage(ctx context.Context, msg JobMessage) error {
	b, err := msgpack.Marshal(msg)
	if err != nil {
		return err
	}

	return s.broker.Enqueue(ctx, b, msg.Queue)
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
