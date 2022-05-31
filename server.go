package tasqueue

import (
	"context"
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

// GetResult() accepts a UUID and returns the result of the job in the results store.
func (s *Server) GetResult(ctx context.Context, uuid string) ([]byte, error) {
	b, err := s.results.Get(ctx, resultsPrefix+uuid)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// Start() starts the job consumer and processor. It is a blocking function.
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
			task, err := s.getHandler(msg.Job.Task)
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
			return s.retryJob(ctx, msg)
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

	// If the task contains OnSuccess task (part of a chain), enqueue them.
	if msg.Job.OnSuccess != nil {
		msg.OnSuccessUUID, err = s.Enqueue(ctx, *msg.Job.OnSuccess)
		if err != nil {
			return err
		}
	}

	return s.statusDone(ctx, msg)
}

// retryJob() increments the retried count and re-queues the task message.
func (s *Server) retryJob(ctx context.Context, msg JobMessage) error {

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
