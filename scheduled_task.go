package tasqueue

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"

	"github.com/sirupsen/logrus"
)

// scheduledJob holds the broker & results interfaces required to enqeueue a task.
// It has a Run() method that enqeues the task. This method is called by the cron scheduler.
type scheduledJob struct {
	log     *logrus.Logger
	ctx     context.Context
	broker  Broker
	results Results
	task    *Job
}

// newScheduled returns a scheduledTask used by the cron scheduler.
func newScheduled(ctx context.Context, log *logrus.Logger, b Broker, r Results, t *Job) *scheduledJob {
	return &scheduledJob{
		log:     log,
		ctx:     ctx,
		broker:  b,
		results: r,
		task:    t,
	}
}

// Run() lets scheduledTask implement the cron job interface.
// It uses the embedded broker and results interfaces to enqueue a task.
// Functionally, this method is similar to server.AddTask().
func (s *scheduledJob) Run() {
	// Set task status in the results backend.
	var (
		buff    bytes.Buffer
		encoder = gob.NewEncoder(&buff)
		msg     = s.task.message()
	)
	msg.setProcessedNow()
	msg.Status = statusStarted

	b, err := json.Marshal(msg)
	if err != nil {
		s.log.Error("could not marshal task message", err)
		return
	}
	if err := s.results.Set(s.ctx, msg.UUID, b); err != nil {
		s.log.Error("could not set task status", err)
		return
	}
	if err := encoder.Encode(msg); err != nil {
		s.log.Error("could not encode task message", err)
		return
	}
	if err := s.broker.Enqueue(s.ctx, buff.Bytes(), s.task.opts.queue); err != nil {
		s.log.Error("could not enqueue task message", err)
	}

}
