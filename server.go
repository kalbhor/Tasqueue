package tasqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace"
	spans "go.opentelemetry.io/otel/trace"
)

const (
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

	// name used to identify this instrumentation library.
	tracer = "tasqueue"
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
	RetryingCB   func(JobCtx, error)
	FailedCB     func(JobCtx, error)
}

// RegisterTask maps a new task against the tasks map on the server.
// It accepts different options for the task (to set callbacks).
func (s *Server) RegisterTask(name string, fn handler, opts TaskOpts) error {
	s.log.Debug("registered handler", "name", name, "options", opts)

	if opts.Queue == "" {
		opts.Queue = DefaultQueue
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = uint32(s.defaultConc)
	}

	s.q.RLock()
	conc, ok := s.queues[opts.Queue]
	s.q.RUnlock()
	if !ok {
		s.registerQueue(opts.Queue, opts.Concurrency)
		s.registerHandler(name, Task{name: name, handler: fn, opts: opts})

		return nil

	}

	// If the queue is already defined and the passed concurrency optional
	// is same (it can be default queue/conc) so simply register the handler
	if opts.Concurrency == conc {
		s.registerHandler(name, Task{name: name, handler: fn, opts: opts})
		return nil
	}

	// If queue is already registered but a new conc was defined, return an err
	return fmt.Errorf("queue is already defined with %d concurrency", conc)
}

// Server is the main store that holds the broker and the results communication interfaces.
// It also stores the registered tasks.
type Server struct {
	log       *slog.Logger
	broker    Broker
	results   Results
	cron      *cron.Cron
	traceProv *trace.TracerProvider

	p      sync.RWMutex
	tasks  map[string]Task
	q      sync.RWMutex
	queues map[string]uint32

	defaultConc int
}

type ServerOpts struct {
	Broker        Broker
	Results       Results
	Logger        slog.Handler
	TraceProvider *trace.TracerProvider
}

// NewServer() returns a new instance of server, with sane defaults.
func NewServer(o ServerOpts) (*Server, error) {
	if o.Broker == nil {
		return nil, fmt.Errorf("broker missing in options")
	}
	if o.Results == nil {
		return nil, fmt.Errorf("results missing in options")
	}
	if o.Logger == nil {
		o.Logger = slog.Default().Handler()
	}

	return &Server{
		traceProv:   o.TraceProvider,
		log:         slog.New(o.Logger),
		cron:        cron.New(),
		broker:      o.Broker,
		results:     o.Results,
		tasks:       make(map[string]Task),
		defaultConc: runtime.GOMAXPROCS(0),
		queues:      make(map[string]uint32),
	}, nil
}

// GetTasks() returns a list of all tasks registered with the server.
func (s *Server) GetTasks() []string {
	s.p.RLock()
	defer s.p.RUnlock()

	t := make([]string, 0, len(s.tasks))
	for name := range s.tasks {
		t = append(t, name)
	}

	return t
}

var ErrNotFound = errors.New("result not found")

// GetResult() accepts a ID and returns the result of the job in the results store.
func (s *Server) GetResult(ctx context.Context, id string) ([]byte, error) {
	b, err := s.results.Get(ctx, id)
	if err == nil {
		return b, nil
	}

	// Check if error is due to key being invalid
	if errors.Is(err, s.results.NilError()) {
		return nil, ErrNotFound
	}

	return nil, err
}

// GetPending() returns the pending job message's in the broker's queue.
func (s *Server) GetPending(ctx context.Context, queue string) ([]JobMessage, error) {
	rs, err := s.broker.GetPending(ctx, queue)
	if err != nil {
		return nil, err
	}

	var jobMsg = make([]JobMessage, len(rs))
	for i, r := range rs {
		if err := msgpack.Unmarshal([]byte(r), &jobMsg[i]); err != nil {
			return nil, err
		}
	}

	return jobMsg, nil
}

// DeleteJob() removes the stored results of a particular job. It does not "dequeue"
// an unprocessed job. It is useful for removing the status of old finished jobs.
func (s *Server) DeleteJob(ctx context.Context, id string) error {
	return s.results.DeleteJob(ctx, id)
}

// GetFailed() returns the list of ids of jobs that failed.
func (s *Server) GetFailed(ctx context.Context) ([]string, error) {
	return s.results.GetFailed(ctx)
}

// GetSuccess() returns the list of ids of jobs that were successful.
func (s *Server) GetSuccess(ctx context.Context) ([]string, error) {
	return s.results.GetSuccess(ctx)
}

// Start() starts the job consumer and processor. It is a blocking function.
func (s *Server) Start(ctx context.Context) {
	// Loop over each registered queue.
	s.q.RLock()
	queues := s.queues
	s.q.RUnlock()

	var wg sync.WaitGroup

	s.startCronScheduler(ctx, &wg)
	for q, conc := range queues {
		q := q // Hack to fix the loop variable capture issue.
		if s.traceProv != nil {
			var span spans.Span
			ctx, span = otel.Tracer(tracer).Start(ctx, "start")
			defer span.End()
		}

		work := make(chan []byte)
		wg.Add(1)
		go func() {
			s.consume(ctx, work, q)
			wg.Done()
		}()

		for i := 0; i < int(conc); i++ {
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
		var span spans.Span
		if s.traceProv != nil {
			ctx, span = otel.Tracer(tracer).Start(ctx, "process")
			defer span.End()
		}

		select {
		case <-ctx.Done():
			s.log.Info("shutting down processor..")
			return
		case work := <-w:
			var (
				msg JobMessage
				err error
			)
			// Decode the bytes into a job message
			if err = msgpack.Unmarshal(work, &msg); err != nil {
				s.spanError(span, err)
				s.log.Error("error unmarshalling task", "error", err)
				break
			}

			// Fetch the registered task handler.
			task, err := s.getHandler(msg.Job.Task)
			if err != nil {
				s.spanError(span, err)
				s.log.Error("handler not found", "error", err)
				break
			}

			// Set the job status as being "processed"
			if err := s.statusProcessing(ctx, msg); err != nil {
				s.spanError(span, err)
				s.log.Error("error setting the status to processing", "error", err)
				break
			}

			if err := s.execJob(ctx, msg, task); err != nil {
				s.spanError(span, err)
				s.log.Error("could not execute job", "error", err)
			}
		}
	}
}

func (s *Server) execJob(ctx context.Context, msg JobMessage, task Task) error {
	var span spans.Span
	if s.traceProv != nil {
		ctx, span = otel.Tracer(tracer).Start(ctx, "exec_job")
		defer span.End()
	}
	// Create the task context, which will be passed to the handler.
	// TODO: maybe use sync.Pool
	taskCtx := JobCtx{Meta: msg.Meta, store: s.results}
	var (
		// errChan is to receive the error returned by the handler.
		errChan = make(chan error, 1)
		err     error

		// jctx is the context passed to the job.
		jctx, cancelFunc = context.WithCancel(ctx)
	)

	// If there is a deadline given, set that on jctx and not ctx
	// because we don't want to cancel the entire context in case deadline exceeded.
	if !(msg.Job.Opts.Timeout == 0) {
		jctx, cancelFunc = context.WithDeadline(ctx, time.Now().Add(msg.Job.Opts.Timeout))
	}

	// Set jctx as the context for the task.
	taskCtx.Context = jctx

	if task.opts.ProcessingCB != nil {
		task.opts.ProcessingCB(taskCtx)
	}

	go func() {
		errChan <- task.handler(msg.Job.Payload, taskCtx)
		close(errChan)
	}()

	select {
	case <-jctx.Done():
		cancelFunc()
		err = jctx.Err()
		if jctx.Err() == context.Canceled {
			err = nil
		}
	case jerr := <-errChan:
		cancelFunc()
		err = jerr
		if jerr == context.Canceled {
			err = nil
		}
	}

	if err != nil {
		// Set the job's error
		msg.PrevErr = err.Error()
		// Try queueing the job again.
		if msg.MaxRetry != msg.Retried {
			if task.opts.RetryingCB != nil {
				task.opts.RetryingCB(taskCtx, err)
			}
			return s.retryJob(ctx, msg)
		} else {
			if task.opts.FailedCB != nil {
				task.opts.FailedCB(taskCtx, err)
			}

			// If there are jobs to enqueued after failure, enqueue them.
			if msg.Job.OnError != nil {
				// Extract OnErrorJob into a variable to get opts.
				for _, j := range msg.Job.OnError {
					nj := *j
					meta := DefaultMeta(nj.Opts)

					if _, err = s.enqueueWithMeta(ctx, nj, meta); err != nil {
						return fmt.Errorf("error enqueuing jobs after failure: %w", err)
					}
				}
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
		for _, j := range msg.Job.OnSuccess {
			// Extract OnSuccessJob into a variable to get opts.
			nj := *j
			meta := DefaultMeta(nj.Opts)
			meta.PrevJobResult, err = s.GetResult(ctx, msg.ID)
			if err != nil {
				return err
			}

			// Set the ID of the next job in the chain
			onSuccessID, err := s.enqueueWithMeta(ctx, nj, meta)
			if err != nil {
				return err
			}

			msg.OnSuccessIDs = append(msg.OnSuccessIDs, onSuccessID)
		}
	}

	if err := s.statusDone(ctx, msg); err != nil {
		s.spanError(span, err)
		return err
	}

	return nil
}

// retryJob() increments the retried count and re-queues the task message.
func (s *Server) retryJob(ctx context.Context, msg JobMessage) error {
	var span spans.Span
	if s.traceProv != nil {
		ctx, span = otel.Tracer(tracer).Start(ctx, "retry_job")
		defer span.End()
	}

	msg.Retried += 1
	b, err := msgpack.Marshal(msg)
	if err != nil {
		s.spanError(span, err)
		return err
	}

	if err := s.statusRetrying(ctx, msg); err != nil {
		s.spanError(span, err)
		return err
	}

	if err := s.broker.Enqueue(ctx, b, msg.Queue); err != nil {
		s.spanError(span, err)
		return err
	}

	return nil
}

func (s *Server) registerQueue(name string, conc uint32) {
	s.q.Lock()
	s.queues[name] = conc
	s.q.Unlock()
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
	var span spans.Span
	if s.traceProv != nil {
		ctx, span = otel.Tracer(tracer).Start(ctx, "status_started")
		defer span.End()
	}

	t.ProcessedAt = time.Now()
	t.Status = StatusStarted

	if err := s.setJobMessage(ctx, t); err != nil {
		s.spanError(span, err)
		return err
	}

	return nil
}

func (s *Server) statusProcessing(ctx context.Context, t JobMessage) error {
	var span spans.Span
	if s.traceProv != nil {
		ctx, span = otel.Tracer(tracer).Start(ctx, "status_processing")
		defer span.End()
	}

	t.ProcessedAt = time.Now()
	t.Status = StatusProcessing

	if err := s.setJobMessage(ctx, t); err != nil {
		s.spanError(span, err)
		return err
	}

	return nil
}

func (s *Server) statusDone(ctx context.Context, t JobMessage) error {
	var span spans.Span
	if s.traceProv != nil {
		ctx, span = otel.Tracer(tracer).Start(ctx, "status_done")
		defer span.End()
	}

	t.ProcessedAt = time.Now()
	t.Status = StatusDone

	if err := s.results.SetSuccess(ctx, t.ID); err != nil {
		return err
	}

	if err := s.setJobMessage(ctx, t); err != nil {
		s.spanError(span, err)
		return err
	}

	return nil
}

func (s *Server) statusFailed(ctx context.Context, t JobMessage) error {
	var span spans.Span
	if s.traceProv != nil {
		ctx, span = otel.Tracer(tracer).Start(ctx, "status_failed")
		defer span.End()
	}

	t.ProcessedAt = time.Now()
	t.Status = StatusFailed

	if err := s.results.SetFailed(ctx, t.ID); err != nil {
		return err
	}

	if err := s.setJobMessage(ctx, t); err != nil {
		s.spanError(span, err)
		return err
	}

	return nil
}

func (s *Server) statusRetrying(ctx context.Context, t JobMessage) error {
	var span spans.Span
	if s.traceProv != nil {
		ctx, span = otel.Tracer(tracer).Start(ctx, "status_retrying")
		defer span.End()
	}

	t.ProcessedAt = time.Now()
	t.Status = StatusRetrying

	if err := s.setJobMessage(ctx, t); err != nil {
		s.spanError(span, err)
		return err
	}

	return nil
}

// spanError checks if tracing is enabled & adds an error to
// supplied span.
func (s *Server) spanError(sp spans.Span, err error) {
	if s.traceProv != nil {
		sp.RecordError(err)
		sp.SetStatus(codes.Error, err.Error())
	}
}

// startCronScheduler starts the cron scheduler and manages its lifecycle.
// It handles graceful shutdown when the context is cancelled.
func (s *Server) startCronScheduler(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Check if context is already cancelled before starting cron
		select {
		case <-ctx.Done():
			s.log.Debug("context cancelled before cron start")
			return
		default:
		}

		s.cron.Start()
		s.log.Debug("cron scheduler started")

		// Wait for shutdown signal
		<-ctx.Done()
		s.log.Info("shutting down cron scheduler...")

		// Stop cron with timeout protection
		cronCtx := s.cron.Stop()
		shutdownTimeout := 5 * time.Second

		select {
		case <-cronCtx.Done():
			s.log.Debug("cron scheduler stopped gracefully")
		case <-time.After(shutdownTimeout):
			s.log.Warn("cron scheduler stop timeout - forcing exit")
		}
	}()
}
