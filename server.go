package tasqueue

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
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
type Handler func([]byte) error

// Opts is an interface to define arbitratry options.
type Opts interface {
	Name() string
	Value() interface{}
}

// Server is the main store that holds the broker and the results communication channels.
// The server also has methods to insert and consume tasks.
type Server struct {
	log     *logrus.Logger
	broker  Broker
	results Results
	cron    *cron.Cron

	p          sync.RWMutex
	processors map[string]Handler
}

// NewServer() returns a new instance of server with redis broker and result.
// It also initialises the error and info loggers.
func NewServer(b Broker, r Results) *Server {
	return &Server{
		log:        logrus.New(),
		cron:       cron.New(),
		broker:     b,
		results:    r,
		processors: make(map[string]Handler),
	}
}

// AddTask() accepts a context and a task. It converts the task into a task message
// which is queued onto the broker.
func (s *Server) AddTask(ctx context.Context, t *Task) error {
	s.log.Debugf("added task : %v", t)
	// Task is converted into a TaskMessage, gob encoded and queued onto the broker
	var (
		b       bytes.Buffer
		encoder = gob.NewEncoder(&b)
		msg     = t.message()
	)
	if t.schedule != "" {
		sTask := newScheduled(ctx, s.log, s.broker, s.results, t)
		// TODO: maintain a map of scheduled cron tasks
		if _, err := s.cron.AddJob(t.schedule, sTask); err != nil {
			return err
		}
	} else {
		// Set task status in the results backend.
		s.statusStarted(ctx, &msg)
		if err := encoder.Encode(msg); err != nil {
			return err
		}
		if err := s.broker.Enqueue(ctx, b.Bytes(), t.queue); err != nil {
			return err
		}
	}

	return nil
}

// retryTask() increments the retried count and re-queues the task message.
func (s *Server) retryTask(ctx context.Context, msg *TaskMessage) error {
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

	return s.broker.Enqueue(ctx, b.Bytes(), msg.Task.queue)
}

// GetTask() accepts a UUID and returns the task message in the results store.
// This is useful to check the status of a task message.
func (s *Server) GetTask(ctx context.Context, uuid string) (*TaskMessage, error) {
	s.log.Debugf("getting task : %s", uuid)

	b, err := s.results.Get(ctx, uuid)
	if err != nil {
		return nil, err
	}

	var t TaskMessage
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, err
	}

	return &t, nil
}

// RegisterProcessor() registers a processing method.
func (s *Server) RegisterProcessor(name string, fn Handler) {
	s.log.Debugf("added handler: %s", name)
	s.registerHandler(name, fn)
}

// Start() starts the task consumer and processor.
func (s *Server) Start(ctx context.Context, opts ...Opts) {
	var (
		concurrency uint32      = defaultConcurrency
		queue       string      = DefaultQueue
		work        chan []byte = make(chan []byte)
	)

	for _, v := range opts {
		switch v.Name() {
		case concurrencyOpt:
			concurrency = v.Value().(uint32)
		case customQueueOpt:
			queue = v.Value().(string)
		default:
			s.log.Errorf("ignoring invalid option: %s", v.Name())
		}
	}

	go s.cron.Start()
	go s.consume(ctx, work, queue)

	var wg sync.WaitGroup
	for i := 0; uint32(i) < concurrency; i++ {
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
				msg TaskMessage
				err error
			)
			decoder := gob.NewDecoder(bytes.NewBuffer(work))
			err = decoder.Decode(&msg)
			if err != nil {
				s.log.Error("error unmarshalling task", err)
				break
			}
			fn, err := s.getHandler(msg.Task.Handler)
			if err != nil {
				s.log.Error("handler not found.", err)
				break
			}
			s.statusProcessing(ctx, &msg)
			err = fn(msg.Task.Payload)
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
				if msg.Task.OnSuccess != nil {
					for _, t := range msg.Task.OnSuccess {
						s.AddTask(ctx, t)
					}
				}
			}
		}
	}
}

func (s *Server) registerHandler(name string, fn Handler) {
	s.p.Lock()
	s.processors[name] = fn
	s.p.Unlock()
}

func (s *Server) getHandler(name string) (Handler, error) {
	s.p.Lock()
	fn, ok := s.processors[name]
	s.p.Unlock()
	if !ok {
		return nil, errHandlerNotFound
	}

	return fn, nil
}

func (s *Server) statusStarted(ctx context.Context, t *TaskMessage) {
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

func (s *Server) statusProcessing(ctx context.Context, t *TaskMessage) {
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

func (s *Server) statusDone(ctx context.Context, t *TaskMessage) {
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

func (s *Server) statusFailed(ctx context.Context, t *TaskMessage) {
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

func (s *Server) statusRetrying(ctx context.Context, t *TaskMessage) {
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
