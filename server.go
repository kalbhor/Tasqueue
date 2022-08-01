package tasqueue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/zerodha/logf"
)

const (
	// name used to identify this instrumentation library.
	tracer = "tasqueue"

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

	opts TaskOpts
}

type TaskOpts struct {
	Concurrency  uint32
	Queue        string
	SuccessCB    func(JobCtx)
	ProcessingCB func(JobCtx)
	RetryingCB   func(JobCtx)
	FailedCB     func(JobCtx)
}

// RegisterTask maps a new task against the tasks map on the server.
// It accepts different options for the task (to set callbacks).
func (s *Server) RegisterTask(name string, fn handler, opts TaskOpts) {
	s.log.Debug("added handler", "name", name)

	if opts.Concurrency <= 0 {
		opts.Concurrency = defaultConcurrency
	}
	if opts.Queue == "" {
		opts.Queue = DefaultQueue
	}

	s.registerHandler(name, Task{name: name, handler: fn, opts: opts})
}

// Server is the main store that holds the broker and the results communication interfaces.
// It also stores the registered tasks.
type Server struct {
	log     logf.Logger
	broker  Broker
	results Results
	cron    *cron.Cron

	p     sync.RWMutex
	tasks map[string]Task
}

// ServerOpts are curated options for the server
type ServerOpts struct {
	Tracing       bool
	TraceProvider *trace.TracerProvider
}

// NewServer() returns a new instance of server, with sane defaults.
func NewServer(b Broker, r Results, logger logf.Logger) (*Server, error) {
	return &Server{
		log:     logger,
		cron:    cron.New(),
		broker:  b,
		results: r,
		tasks:   make(map[string]Task),
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
	go s.cron.Start()
	// Loop over each registered task.
	s.p.RLock()
	tasks := s.tasks
	s.p.RUnlock()

	var wg sync.WaitGroup
	for _, task := range tasks {
		if s.opts.Tracing {
			var span spans.Span
			ctx, span = otel.Tracer(tracer).Start(ctx, "Start")
			defer span.End()
		}

		task := task
		work := make(chan []byte)
		wg.Add(1)
		go func() {
			s.consume(ctx, work, task.opts.Queue)
			wg.Done()
		}()

		for i := 0; i < int(task.opts.Concurrency); i++ {
			wg.Add(1)
			go func() {
				s.process(ctx, work)
				wg.Done()
			}()
		}
	}
	wg.Wait()
}

// consume() listens on the queue for task messages and passes the task to processor.
func (s *Server) consume(ctx context.Context, work chan []byte, queue string) {
	s.log.Debug("starting task consumer..")
	s.broker.Consume(ctx, work, queue)
}

// process() listens on the work channel for tasks. On receiving a task it checks the
// processors map and passes payload to relevant processor.
func (s *Server) process(ctx context.Context, w chan []byte) {
	s.log.Debug("starting processor..")
	for {
		if s.opts.Tracing {
			var span spans.Span
			ctx, span = otel.Tracer(tracer).Start(ctx, "process")
			defer span.End()
		}

		select {
		case <-ctx.Done():
			s.log.Debug("shutting down processor..")
			return
		case work := <-w:
			var (
				msg JobMessage
				err error
			)
			// Decode the bytes into a job message
			if err = msgpack.Unmarshal(work, &msg); err != nil {
				s.log.Error("error unmarshalling task", "error", err)
				break
			}
			// Fetch the registered task handler.
			task, err := s.getHandler(msg.Job.Task)
			if err != nil {
				s.log.Error("handler not found", "error", err)
				break
			}

			// Set the job status as being "processed"
			if err := s.statusProcessing(ctx, msg); err != nil {
				s.log.Error("error setting the status to processing", "error", err)
				break
			}

			if err := s.execJob(ctx, msg, task); err != nil {
				s.log.Error("could not execute job. err", "error", err)
			}
		}
	}
}

func (s *Server) execJob(ctx context.Context, msg JobMessage, task Task) error {
	if s.opts.Tracing {
		var span spans.Span
		ctx, span = otel.Tracer(tracer).Start(ctx, "execJob")
		defer span.End()
	}
	// Create the task context, which will be passed to the handler.
	// TODO: maybe use sync.Pool
	taskCtx := JobCtx{Meta: msg.Meta, store: s.results}

	if task.opts.ProcessingCB != nil {
		task.opts.ProcessingCB(taskCtx)
	}

	err := task.handler(msg.Job.Payload, taskCtx)
	if err != nil {
		// Set the job's error
		msg.PrevErr = err.Error()
		// Try queueing the job again.
		if msg.MaxRetry != msg.Retried {
			if task.opts.RetryingCB != nil {
				task.opts.RetryingCB(taskCtx)
			}
			return s.retryJob(ctx, msg)
		} else {
			if task.opts.FailedCB != nil {
				task.opts.FailedCB(taskCtx)
			}
			// If we hit max retries, set the task status as failed.
			return s.statusFailed(ctx, msg)
		}
	}

	if task.opts.SuccessCB != nil {
		task.opts.SuccessCB(taskCtx)
	}

	// If the task contains OnSuccess task (part of a chain), enqueue them.
	if msg.Job.OnSuccess != nil {
		// Extract OnSuccessJob into a variable to get opts.
		j := msg.Job.OnSuccess
		nj := *j
		meta := DefaultMeta(nj.opts)
		meta.PrevJobResults = taskCtx.results
		msg.OnSuccessUUID, err = s.enqueueWithMeta(ctx, nj, meta)
		if err != nil {
			return err
		}
	}

	return s.statusDone(ctx, msg)
}

// retryJob() increments the retried count and re-queues the task message.
func (s *Server) retryJob(ctx context.Context, msg JobMessage) error {
	if s.opts.Tracing {
		var span spans.Span
		ctx, span = otel.Tracer(tracer).Start(ctx, "retryJob")
		defer span.End()
	}

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
	s.p.RLock()
	fn, ok := s.tasks[name]
	s.p.RUnlock()
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
	if s.opts.Tracing {
		var span spans.Span
		ctx, span = otel.Tracer(tracer).Start(ctx, "statusProcessing")
		defer span.End()
	}

	t.ProcessedAt = time.Now()
	t.Status = StatusProcessing

	return s.setJobMessage(ctx, t)
}

func (s *Server) statusDone(ctx context.Context, t JobMessage) error {
	if s.opts.Tracing {
		var span spans.Span
		ctx, span = otel.Tracer(tracer).Start(ctx, "statusDone")
		defer span.End()
	}

	t.ProcessedAt = time.Now()
	t.Status = StatusDone

	return s.setJobMessage(ctx, t)
}

func (s *Server) statusFailed(ctx context.Context, t JobMessage) error {
	if s.opts.Tracing {
		var span spans.Span
		ctx, span = otel.Tracer(tracer).Start(ctx, "statusFailed")
		defer span.End()
	}

	t.ProcessedAt = time.Now()
	t.Status = StatusFailed

	return s.setJobMessage(ctx, t)
}

func (s *Server) statusRetrying(ctx context.Context, t JobMessage) error {
	if s.opts.Tracing {
		var span spans.Span
		ctx, span = otel.Tracer(tracer).Start(ctx, "statusRetrying")
		defer span.End()
	}

	t.ProcessedAt = time.Now()
	t.Status = StatusRetrying

	return s.setJobMessage(ctx, t)
}
