package tasqueue

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"sync"

	"go.zerodha.tech/commons/go-logger"
	redis_broker "go.zerodha.tech/kalbhor/kronika/brokers/redis"
	redis_results "go.zerodha.tech/kalbhor/kronika/results/redis"
)

const (
	defaultConcurrency = 1
	statusStarted      = "started"
	statusProcessing   = "processing"
	statusFailed       = "failed"
	statusDone         = "successful"
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
	infoLog *logger.Logger
	errLog  *logger.Logger
	broker  Broker
	results Results

	p          sync.RWMutex
	processors map[string]Handler
}

// NewServer() returns a new instance of server with redis broker and result.
// It also initialises the error and info loggers.
func NewServer() *Server {
	return &Server{
		infoLog:    logger.New(logger.Info, "kronika-info"),
		errLog:     logger.New(logger.Production, "kronika-error"),
		broker:     redis_broker.New(redis_broker.DefaultRedis(), nil, nil),
		results:    redis_results.New(redis_results.DefaultRedis()),
		processors: make(map[string]Handler),
	}
}

// AddTask() accepts a context and a task. It converts the task into a task message
// which is queued onto the broker.
func (s *Server) AddTask(ctx context.Context, t *Task) error {
	s.infoLog.InfoWith("added task..").Any("task", t).Write()

	// Task is converted into a TaskMessage, gob encoded and queued onto the broker
	var (
		b       bytes.Buffer
		encoder = gob.NewEncoder(&b)
		msg     = t.message()
	)
	// Set task status in the results backend.
	s.statusStarted(ctx, msg)
	if err := encoder.Encode(msg); err != nil {
		return err
	}

	return s.broker.Enqueue(ctx, b.Bytes(), t.Queue)
}

// GetTask() accepts a UUID and returns the task message in the results store.
// This is useful to check the status of a task message.
func (s *Server) GetTask(ctx context.Context, uuid string) (*TaskMessage, error) {
	s.infoLog.InfoWith("getting task..").String("uuid", uuid).Write()
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
	s.infoLog.InfoWith("added processor..").String("name", name).Write()
	s.registerHandler(name, fn)
}

// Start() starts the task consumer and processor.
func (s *Server) Start(ctx context.Context, opts ...Opts) {
	var (
		concurrency uint32      = defaultConcurrency
		queue       string      = defaultQueue
		work        chan []byte = make(chan []byte)
	)

	for _, v := range opts {
		switch v.Name() {
		case concurrencyOpt:
			concurrency = v.Value().(uint32)
		case customQueueOpt:
			queue = v.Value().(string)
		default:
			s.infoLog.Info("ignoring invalid option")
		}
	}

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
	s.infoLog.Info("starting task consumer..")
	s.broker.Consume(ctx, work, queue)
}

// process() listens on the work channel for tasks. On receiving a task it checks the
// processors map and passes payload to relevant processor.
func (s *Server) process(ctx context.Context, work chan []byte, wg *sync.WaitGroup) {
	s.infoLog.Info("starting processor..")
	for {
		select {
		case <-ctx.Done():
			s.infoLog.Info("shutting down processor..")
			wg.Done()
			return
		case work := <-work:
			var (
				msg TaskMessage
				err error
			)
			decoder := gob.NewDecoder(bytes.NewBuffer(work))
			err = decoder.Decode(&msg)
			if err != nil {
				s.errLog.ErrGeneral("error unmarshalling task", err).Write()
				break
			}
			fn, err := s.getHandler(msg.Task.Handler)
			if err != nil {
				s.errLog.ErrGeneral("handler not found.", err).String("task", msg.Task.Handler).Write()
				break
			}
			s.statusProcessing(ctx, &msg)
			// TODO: check error and try retrying
			fn(msg.Task.Payload)
			s.statusDone(ctx, &msg)
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
	t.Status = statusStarted
	b, err := json.Marshal(t)
	if err != nil {
		s.errLog.ErrGeneral("could not marshal task message", err).Any("task_message", t).Write()
		return
	}
	if err := s.results.Set(ctx, t.UUID, b); err != nil {
		s.errLog.ErrGeneral("could not set task status", err).Any("task", t).Write()
	}
}

func (s *Server) statusProcessing(ctx context.Context, t *TaskMessage) {
	t.Status = statusProcessing
	b, err := json.Marshal(t)
	if err != nil {
		s.errLog.ErrGeneral("could not marshal task message", err).Any("task_message", t).Write()
		return
	}
	if err := s.results.Set(ctx, t.UUID, b); err != nil {
		s.errLog.ErrGeneral("could not set task status", err).Any("task", t).Write()
	}
}

func (s *Server) statusDone(ctx context.Context, t *TaskMessage) {
	t.Status = statusDone
	b, err := json.Marshal(t)
	if err != nil {
		s.errLog.ErrGeneral("could not marshal task message", err).Any("task_message", t).Write()
		return
	}
	if err := s.results.Set(ctx, t.UUID, b); err != nil {
		s.errLog.ErrGeneral("could not set task status", err).Any("task", t).Write()
	}
}

func (s *Server) statusFailed(ctx context.Context, t *TaskMessage) {
	t.Status = statusFailed
	b, err := json.Marshal(t)
	if err != nil {
		s.errLog.ErrGeneral("could not marshal task message", err).Any("task_message", t).Write()
		return
	}
	if err := s.results.Set(ctx, t.UUID, b); err != nil {
		s.errLog.ErrGeneral("could not set task status", err).Any("task", t).Write()
	}
}
