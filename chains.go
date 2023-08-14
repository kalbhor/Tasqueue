package tasqueue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// ChainMeta contains fields related to a chain job.
type ChainMeta struct {
	ID string
	// Status of the overall chain
	Status string
	// ID of the current job part of chain
	JobID string
	// List of IDs of completed jobs
	PrevJobs []string
}

// ChainMessage is a wrapper over Chain, containing meta info such as status, id.
// A ChainMessage is stored in the results store.
type ChainMessage struct {
	ChainMeta
}

type Chain struct {
	Jobs []Job
	Opts ChainOpts
}

type ChainOpts struct {
	// Optional ID passed by client. If empty, Tasqueue generates it.
	ID string
}

// NewChain() accepts a list of Tasks and creates a chain by setting the
// onSuccess task of i'th task to (i+1)'th task, hence forming a "chain".
// It returns the first task (essentially the first node of the linked list), which can be queued normally.
func NewChain(j []Job, opts ChainOpts) (Chain, error) {
	if len(j) < 2 {
		return Chain{}, fmt.Errorf("minimum 2 tasks required to form chain")
	}

	// Set the on success tasks as the i+1 task,
	// hence forming a "chain" of tasks.
	for i := 0; i < len(j)-1; i++ {
		j[i].OnSuccess = &j[i+1]
	}

	return Chain{Jobs: j, Opts: opts}, nil
}

// message() converts a group into a group message, ready to be enqueued/stored.
func (c *Chain) message() ChainMessage {
	if c.Opts.ID == "" {
		c.Opts.ID = uuid.NewString()
	}

	return ChainMessage{
		ChainMeta: ChainMeta{
			ID:     c.Opts.ID,
			Status: StatusProcessing,
		},
	}
}

func (s *Server) EnqueueChain(ctx context.Context, c Chain) (string, error) {
	msg := c.message()
	root := c.Jobs[0]
	jobID, err := s.Enqueue(ctx, root)
	if err != nil {
		return "", err
	}
	msg.JobID = jobID

	if err := s.setChainMessage(ctx, msg); err != nil {
		return "", err
	}

	return msg.ID, nil
}

func (s *Server) GetChain(ctx context.Context, id string) (ChainMessage, error) {
	c, err := s.getChainMessage(ctx, id)
	if err != nil {
		return ChainMessage{}, err
	}

	if c.Status == StatusDone || c.Status == StatusFailed {
		return c, nil
	}

	// Fetch the current job, to check its status
	currJob, err := s.GetJob(ctx, c.JobID)
	if err != nil {
		return ChainMessage{}, nil
	}

checkJobs:
	switch currJob.Status {
	//If the current job failed, add it to previous jobs list
	// Set the chain status to failed
	case StatusFailed:
		c.PrevJobs = append(c.PrevJobs, currJob.ID)
		c.Status = StatusFailed
	// If the current job status is an intermediatery status
	// Set the chain status as processing.
	case StatusStarted, StatusProcessing, StatusRetrying:
		c.Status = StatusProcessing
	// If the current job status is done, check the next job id.
	// If there is no next job id, the chain is complete, set overall status
	// to success. Otherwise update the current job and perform all the above checks.
	case StatusDone:
		c.PrevJobs = append(c.PrevJobs, currJob.ID)
		if currJob.OnSuccessID == "" {
			c.Status = StatusDone
		} else {
			currJob, err = s.GetJob(ctx, currJob.OnSuccessID)
			if err != nil {
				return ChainMessage{}, nil
			}
			goto checkJobs
		}
	}

	if err = s.setChainMessage(ctx, c); err != nil {
		return ChainMessage{}, nil
	}

	return c, nil
}

const chainPrefix = "chain:msg:"

func (s *Server) setChainMessage(ctx context.Context, c ChainMessage) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return s.results.Set(ctx, chainPrefix+c.ID, b)
}

func (s *Server) getChainMessage(ctx context.Context, id string) (ChainMessage, error) {
	b, err := s.GetResult(ctx, chainPrefix+id)
	if err != nil {
		return ChainMessage{}, err
	}

	var c ChainMessage
	if err := json.Unmarshal(b, &c); err != nil {
		return ChainMessage{}, err
	}

	return c, nil
}
