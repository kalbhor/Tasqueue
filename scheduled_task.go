package tasqueue

import (
	"context"

	"github.com/sirupsen/logrus"
)

// scheduledJob holds the broker & results interfaces required to enqeueue a task.
// It has a Run() method that enqeues the task. This method is called by the cron scheduler.
type scheduledJob struct {
	log    *logrus.Logger
	ctx    context.Context
	broker Broker
	msg    *JobMessage
}

// newScheduled accepts a broker, a byte message and job options
func newScheduled(ctx context.Context, log *logrus.Logger, b Broker, msg *JobMessage) *scheduledJob {
	if log == nil {
		log = logrus.New()
	}
	return &scheduledJob{
		log:    log,
		ctx:    ctx,
		broker: b,
		msg:    msg,
	}
}

// Run() lets scheduledTask implement the cron job interface.
// It uses the embedded broker and results interfaces to enqueue a task.
// Functionally, this method is similar to server.AddTask().
func (s *scheduledJob) Run() {
	if err := s.broker.Enqueue(s.ctx, s.msg.Job.Payload, s.msg.Queue); err != nil {
		s.log.Error("could not enqueue task message", err)
	}

}
