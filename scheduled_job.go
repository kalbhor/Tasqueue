package tasqueue

import (
	"context"

	"github.com/zerodha/logf"
)

// scheduledJob holds the broker & results interfaces required to enqeueue a task.
// It has a Run() method that enqeues the task. This method is called by the cron scheduler.
type scheduledJob struct {
	log    logf.Logger
	ctx    context.Context
	broker Broker
	msg    JobMessage
}

// newScheduled accepts a broker, a byte message and job options
func newScheduled(ctx context.Context, log logf.Logger, b Broker, msg JobMessage) *scheduledJob {
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
