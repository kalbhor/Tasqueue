package tasqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	defaultConcurrency = 1

	// This is the initial state when a job is pushed onto the broker.
	StatusStarted = "queued"

	// This is the state when a worker has recieved a job.
	StatusProcessing = "processing"

	// The state when a job completes, but returns an error (and all retries are over).
	StatusFailed = "failed"

	// The state when a job completes without any error.
	StatusDone = "successful"

	// The state when a job errors out and is queued again to be retried.
	// This state is analogous to statusStarted.
	StatusRetrying = "retrying"
)

// handler represents a function that can accept arbitrary payload
// and process it in any manner. A job ctx is passed, which allows the handler access to job details
// and lets it save arbitrary results using JobCtx.Save()
type handler func([]byte, JobCtx) error

// Task is a pre-registered job handler. It stores the callbacks (set through options), which are
// called during different states of a job.
type Task struct {
	name    string
	handler handler

	successCB    func(JobCtx)
	processingCB func(JobCtx)
	retryingCB   func(JobCtx)
	failedCB     func(JobCtx)
}

// RegisterTask maps a new task against the tasks map on the server.
// It accepts different options for the task (to set callbacks).
func (s *Server) RegisterTask(name string, fn handler, opts ...Opts) {
	s.log.Infof("added handler: %s", name)

	var t = Task{name: name, handler: fn}

	for _, v := range opts {
		switch v.Name() {
		case successCallback:
			t.successCB = v.Value().(SuccessCB)
		case failedCallback:
			t.failedCB = v.Value().(FailedCB)
		case processingCallback:
			t.processingCB = v.Value().(ProcessingCB)
		case retryingCallback:
			t.retryingCB = v.Value().(RetryingCB)
		}
	}

	s.registerHandler(name, t)
}

// Server is the main store that holds the broker and the results communication interfaces.
// It also stores the registered tasks.
type Server struct {
	log     *logrus.Logger
	broker  Broker
	results Results
	cron    *cron.Cron

	p     sync.RWMutex
	tasks map[string]Task

	opts serverOpts
}

// serverOpts are curated options to configure a server.
type serverOpts struct {
	concurrency uint32
	queue       string
}

// NewServer() returns a new instance of server, with sane defaults.
func NewServer(b Broker, r Results, opts ...Opts) (*Server, error) {
	var sOpts = serverOpts{concurrency: defaultConcurrency, queue: DefaultQueue}

	for _, v := range opts {
		switch v.Name() {
		case concurrencyOpt:
			sOpts.concurrency = v.Value().(uint32)
		case customQueueOpt:
			sOpts.queue = v.Value().(string)
		default:
			return nil, fmt.Errorf("ignoring invalid option: %s", v.Name())
		}
	}

	return &Server{
		log:     logrus.New(),
		cron:    cron.New(),
		broker:  b,
		results: r,
		tasks:   make(map[string]Task),
		opts:    sOpts,
	}, nil
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

// EnqueueGroup() accepts a group and returns the assigned UUID.
// The following steps take place:
// 1. Converts it into a group message, which assigns a UUID (among other meta info) to the group.
// 2. Sets the group status as "started" on the results store.
// 3. Loops over all jobs part of the group and enqueues the job each job.
// 4. The job status map is updated with the uuids of each enqueued job.
func (s *Server) EnqueueGroup(ctx context.Context, t Group) (string, error) {
	msg := t.message()
	for _, v := range t.Jobs {
		uid, err := s.Enqueue(ctx, v)
		if err != nil {
			return "", fmt.Errorf("could not enqueue group : %w", err)
		}
		msg.JobStatus[uid] = StatusStarted
	}

	if err := s.setGroupMessage(ctx, msg); err != nil {
		return "", err
	}
	return msg.UUID, nil
}

func (s *Server) GetGroup(ctx context.Context, uuid string) (GroupMessage, error) {
	g, err := s.getGroupMessage(ctx, uuid)
	if err != nil {
		return g, nil
	}
	// If the group status is either "done" or "failed".
	// Do an early return
	if g.Status == StatusDone || g.Status == StatusFailed {
		return g, nil
	}

	// jobStatus holds the updated map of job status'
	jobStatus := make(map[string]string)

	// Run over the individual jobs and their status.
	for uuid, status := range g.JobStatus {
		switch status {
		// Jobs with a final status remain the same and do not require lookup
		case StatusFailed, StatusDone:
			jobStatus[uuid] = status
		// Re-look the jobs where the status is an intermediatery state (processing, retrying, etc).
		case StatusStarted, StatusProcessing, StatusRetrying:
			j, err := s.GetJob(ctx, uuid)
			if err != nil {
				return GroupMessage{}, err
			}
			jobStatus[uuid] = j.Status

		}
	}

	// Update the overall group status, based on the individual jobs.
	g.JobStatus = jobStatus
	g.Status = getGroupStatus(jobStatus)

	// Re-set the group result in the store.
	if err := s.setGroupMessage(ctx, g); err != nil {
		return GroupMessage{}, err
	}

	return g, nil
}

func getGroupStatus(jobStatus map[string]string) string {
	status := StatusDone
	for _, st := range jobStatus {
		if st == StatusFailed {
			return StatusFailed
		}
		if st != StatusDone {
			status = StatusProcessing
		}
	}

	return status
}

func (s *Server) setGroupMessage(ctx context.Context, g GroupMessage) error {
	b, err := json.Marshal(g)
	if err != nil {
		return err
	}
	if err := s.results.Set(ctx, g.UUID, b); err != nil {
		return err
	}

	return nil
}

func (s *Server) getGroupMessage(ctx context.Context, uuid string) (GroupMessage, error) {
	b, err := s.results.Get(ctx, uuid)
	if err != nil {
		return GroupMessage{}, err
	}

	var g GroupMessage
	if err := json.Unmarshal(b, &g); err != nil {
		return GroupMessage{}, err
	}

	return g, nil
}

// GetResult() accepts a UUID and returns the result of the job in the results store.
func (s *Server) GetResult(ctx context.Context, uuid string) ([]byte, error) {
	b, err := s.results.Get(ctx, uuid+resultsSuffix)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// Start() starts the task consumer and processor. It is a blocking function.
func (s *Server) Start(ctx context.Context) {
	work := make(chan []byte)

	go s.cron.Start()
	go s.consume(ctx, work, s.opts.queue)

	var wg sync.WaitGroup
	for i := 0; uint32(i) < s.opts.concurrency; i++ {
		wg.Add(1)
		go s.process(ctx, work, &wg)
	}
	wg.Wait()
}

// consume() listens on the queue for task messages and passes the task to processor.
func (s *Server) consume(ctx context.Context, work chan []byte, queue string) {
	s.log.Info("starting task consumer..")
	s.broker.Consume(ctx, work, queue)
}

// process() listens on the work channel for tasks. On receiving a task it checks the
// processors map and passes payload to relevant processor.
func (s *Server) process(ctx context.Context, w chan []byte, wg *sync.WaitGroup) {
	s.log.Info("starting processor..")
	for {
		select {
		case <-ctx.Done():
			s.log.Info("shutting down processor..")
			wg.Done()
			return
		case work := <-w:
			var (
				msg JobMessage
				err error
			)
			// Decode the bytes into a job message
			if err = msgpack.Unmarshal(work, &msg); err != nil {
				s.log.Errorf("error unmarshalling task : %v", err)
				break
			}
			// Fetch the registered task handler.
			task, err := s.getHandler(msg.Job.Handler)
			if err != nil {
				s.log.Errorf("handler not found : %v", err)
				break
			}

			// Set the job status as being "processed"
			if err := s.statusProcessing(ctx, msg); err != nil {
				s.log.Error(err)
				break
			}

			if err := s.execJob(ctx, msg, task); err != nil {
				s.log.Errorf("could not execute job. err : %v", err)
			}
		}
	}
}

func (s *Server) execJob(ctx context.Context, msg JobMessage, task Task) error {
	// Create the task context, which will be passed to the handler.
	// TODO: maybe use sync.Pool
	taskCtx := JobCtx{Meta: msg.Meta, store: s.results}

	if task.processingCB != nil {
		task.processingCB(taskCtx)
	}

	err := task.handler(msg.Job.Payload, taskCtx)
	if err != nil {
		// Set the job's error
		msg.PrevErr = err.Error()
		// Try queueing the job again.
		if msg.MaxRetry != msg.Retried {
			if task.retryingCB != nil {
				task.retryingCB(taskCtx)
			}
			return s.retryTask(ctx, msg)
		} else {
			if task.failedCB != nil {
				task.failedCB(taskCtx)
			}
			// If we hit max retries, set the task status as failed.
			return s.statusFailed(ctx, msg)
		}
	}

	if task.successCB != nil {
		task.successCB(taskCtx)
	}

	// If the task contains OnSuccess tasks (part of a chain), enqueue them.
	if msg.Job.OnSuccess != nil {
		for _, t := range msg.Job.OnSuccess {
			if _, err := s.Enqueue(ctx, t); err != nil {
				return err
			}
		}
	}

	return s.statusDone(ctx, msg)
}

// retryTask() increments the retried count and re-queues the task message.
func (s *Server) retryTask(ctx context.Context, msg JobMessage) error {

	msg.Retried += 1
	b, err := msgpack.Marshal(msg)
	if err != nil {
		return err
	}

	if err := s.statusRetrying(ctx, msg); err != nil {
		return err
	}

	return s.broker.Enqueue(ctx, b, msg.Queue)
}

func (s *Server) registerHandler(name string, t Task) {
	s.p.Lock()
	s.tasks[name] = t
	s.p.Unlock()
}

func (s *Server) getHandler(name string) (Task, error) {
	s.p.Lock()
	fn, ok := s.tasks[name]
	s.p.Unlock()
	if !ok {
		return Task{}, fmt.Errorf("handler %v not found", name)
	}

	return fn, nil
}

func (s *Server) statusStarted(ctx context.Context, t JobMessage) error {
	t.ProcessedAt = time.Now()
	t.Status = StatusStarted

	return s.setJobMessage(ctx, t)
}

func (s *Server) statusProcessing(ctx context.Context, t JobMessage) error {
	t.ProcessedAt = time.Now()
	t.Status = StatusProcessing

	return s.setJobMessage(ctx, t)
}

func (s *Server) statusDone(ctx context.Context, t JobMessage) error {
	t.ProcessedAt = time.Now()
	t.Status = StatusDone

	return s.setJobMessage(ctx, t)
}

func (s *Server) statusFailed(ctx context.Context, t JobMessage) error {
	t.ProcessedAt = time.Now()
	t.Status = StatusFailed

	return s.setJobMessage(ctx, t)
}

func (s *Server) statusRetrying(ctx context.Context, t JobMessage) error {
	t.ProcessedAt = time.Now()
	t.Status = StatusRetrying

	return s.setJobMessage(ctx, t)
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
