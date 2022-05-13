package tasqueue

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
)

const (
	defaultConcurrency = 1
	statusStarted      = "started"
	statusProcessing   = "processing"
	statusFailed       = "failed"
	statusDone         = "successful"
	statusRetrying     = "retrying"
)

var (
	errHandlerNotFound = errors.New("handler not found")
)

// Handler represents a function that can accept arbitrary payload
// and process it any manner.
type Handler func([]byte, *JobCtx) error

// Server is the main store that holds the broker and the results communication channels.
// The server also has methods to insert and consume tasks.
type Server struct {
	log     *logrus.Logger
	broker  Broker
	results Results
	cron    *cron.Cron

	p     sync.RWMutex
	tasks map[string]Handler

	opts serverOpts
}

type serverOpts struct {
	concurrency uint32
	queue       string
}

// NewServer() returns a new instance of server with redis broker and result.
// It also initialises the error and info loggers.
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
		tasks:   make(map[string]Handler),
		opts:    sOpts,
	}, nil
}

// Enqueue() accepts a context and a task. It converts the task into a task message
// which is queued onto the broker.
func (s *Server) Enqueue(ctx context.Context, t *Job) (string, error) {
	s.log.Debugf("added task : %v", t)
	// Task is converted into a TaskMessage, gob encoded and queued onto the broker
	var (
		b       bytes.Buffer
		encoder = gob.NewEncoder(&b)
		msg     = t.message()
	)
	if t.opts.schedule != "" {
		sTask := newScheduled(ctx, s.log, s.broker, s.results, t)
		// TODO: maintain a map of scheduled cron tasks
		if _, err := s.cron.AddJob(t.opts.schedule, sTask); err != nil {
			return "", err
		}
	} else {
		// Set task status in the results backend.
		s.statusStarted(ctx, &msg)
		if err := encoder.Encode(msg); err != nil {
			return "", err
		}
		if err := s.broker.Enqueue(ctx, b.Bytes(), t.opts.queue); err != nil {
			return "", err
		}
	}

	return msg.UUID, nil
}

// retryTask() increments the retried count and re-queues the task message.
func (s *Server) retryTask(ctx context.Context, msg *JobMessage) error {
	s.log.Debugf("retrying task: %v", msg)
	var (
		b       bytes.Buffer
		encoder = gob.NewEncoder(&b)
	)
	msg.Retried += 1
	// Set task status in the results backend.
	s.statusStarted(ctx, msg)
	if err := encoder.Encode(msg); err != nil {
		return err
	}

	return s.broker.Enqueue(ctx, b.Bytes(), msg.Job.opts.queue)
}

// GetTask() accepts a UUID and returns the task message in the results store.
// This is useful to check the status of a task message.
func (s *Server) GetTask(ctx context.Context, uuid string) (*JobMessage, error) {
	s.log.Debugf("getting task : %s", uuid)

	b, err := s.results.Get(ctx, uuid)
	if err != nil {
		return nil, err
	}

	var t JobMessage
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, err
	}

	return &t, nil
}

// GetResult() accepts a UUID and returns the result of the job in the results store.
func (s *Server) GetResult(ctx context.Context, uuid string) ([]byte, error) {
	s.log.Debugf("getting task : %s", uuid)

	b, err := s.results.Get(ctx, uuid+resultsSuffix)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// RegisterTask() registers a processing method.
func (s *Server) RegisterTask(name string, fn Handler) {
	s.log.Debugf("added handler: %s", name)
	s.registerHandler(name, fn)
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
			// Decode the bytes into a task message
			decoder := gob.NewDecoder(bytes.NewBuffer(work))
			err = decoder.Decode(&msg)
			if err != nil {
				s.log.Error("error unmarshalling task", err)
				break
			}
			// Create the task context, which will be passed to the handler.
			taskCtx := &JobCtx{Meta: msg.Meta, store: s.results}

			// Fetch the registered task handler.
			fn, err := s.getHandler(msg.Job.Handler)
			if err != nil {
				s.log.Error("handler not found.", err)
				break
			}

			s.statusProcessing(ctx, &msg)
			err = fn(msg.Job.Payload, taskCtx)
			if err != nil {
				msg.setError(err)
				// Try queueing the task again if an error is returned.
				if msg.MaxRetry != msg.Retried {
					s.statusRetrying(ctx, &msg)
					s.retryTask(ctx, &msg)
				} else {
					// If we hit max retries, set the task status as failed.
					s.statusFailed(ctx, &msg)
				}
			} else {
				s.statusDone(ctx, &msg)
				// If the task contains OnSuccess tasks (part of a chain), enqueue them.
				if msg.Job.OnSuccess != nil {
					for _, t := range msg.Job.OnSuccess {
						s.Enqueue(ctx, t)
					}
				}
			}
		}
	}
}

func (s *Server) registerHandler(name string, fn Handler) {
	s.p.Lock()
	s.tasks[name] = fn
	s.p.Unlock()
}

func (s *Server) getHandler(name string) (Handler, error) {
	s.p.Lock()
	fn, ok := s.tasks[name]
	s.p.Unlock()
	if !ok {
		return nil, errHandlerNotFound
	}

	return fn, nil
}

func (s *Server) statusStarted(ctx context.Context, t *JobMessage) {
	t.setProcessedNow()
	t.Status = statusStarted
	b, err := json.Marshal(t)
	if err != nil {
		s.log.Error("could not marshal task message", err)
		return
	}
	if err := s.results.Set(ctx, t.UUID, b); err != nil {
		s.log.Error("could not set task status", err)
	}
}

func (s *Server) statusProcessing(ctx context.Context, t *JobMessage) {
	t.setProcessedNow()
	t.Status = statusProcessing
	b, err := json.Marshal(t)
	if err != nil {
		s.log.Error("could not marshal task message", err)
		return
	}
	if err := s.results.Set(ctx, t.UUID, b); err != nil {
		s.log.Error("could not set task status", err)
	}
}

func (s *Server) statusDone(ctx context.Context, t *JobMessage) {
	t.setProcessedNow()
	t.Status = statusDone
	b, err := json.Marshal(t)
	if err != nil {
		s.log.Error("could not marshal task message", err)
		return
	}
	if err := s.results.Set(ctx, t.UUID, b); err != nil {
		s.log.Error("could not set task status", err)
	}
}

func (s *Server) statusFailed(ctx context.Context, t *JobMessage) {
	t.setProcessedNow()
	t.Status = statusFailed
	b, err := json.Marshal(t)
	if err != nil {
		s.log.Error("could not marshal task message", err)
		return
	}
	if err := s.results.Set(ctx, t.UUID, b); err != nil {
		s.log.Error("could not set task status", err)
	}
}

func (s *Server) statusRetrying(ctx context.Context, t *JobMessage) {
	t.setProcessedNow()
	t.Status = statusRetrying
	b, err := json.Marshal(t)
	if err != nil {
		s.log.Error("could not marshal task message", err)
		return
	}
	if err := s.results.Set(ctx, t.UUID, b); err != nil {
		s.log.Error("could not set task status", err)
	}
}
