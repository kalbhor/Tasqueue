package tasqueue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

type Group struct {
	Jobs []Job
	Opts GroupOpts
}

type GroupOpts struct {
	ID string
}

// GroupMeta contains fields related to a group job. These are updated when a task is consumed.
type GroupMeta struct {
	ID     string
	Status string
	// JobStatus is a map of job id -> status
	JobStatus map[string]string
}

// GroupMessage is a wrapper over Group, containing meta info such as status, id.
// A GroupMessage is stored in the results store.
type GroupMessage struct {
	GroupMeta
	Group *Group
}

// message() converts a group into a group message, ready to be enqueued/stored.
func (t *Group) message() GroupMessage {
	if t.Opts.ID == "" {
		t.Opts.ID = uuid.NewString()
	}

	return GroupMessage{
		GroupMeta: GroupMeta{
			JobStatus: make(map[string]string),
			ID:        t.Opts.ID,
			Status:    StatusProcessing,
		},
		Group: t,
	}
}

// NewGroup() accepts a list of jobs and creates a group.
func NewGroup(j []Job, opts GroupOpts) (Group, error) {
	return Group{
		Jobs: j,
		Opts: opts,
	}, nil

}

// EnqueueGroup() accepts a group and returns the assigned ID.
// The following steps take place:
// 1. Converts it into a group message, which assigns a ID (among other meta info) to the group.
// 2. Sets the group status as "started" on the results store.
// 3. Loops over all jobs part of the group and enqueues the job each job.
// 4. The job status map is updated with the IDs of each enqueued job.
func (s *Server) EnqueueGroup(ctx context.Context, t Group) (string, error) {
	msg := t.message()
	for _, v := range t.Jobs {
		uid, err := s.Enqueue(ctx, v)
		if err != nil {
			return "", fmt.Errorf("could not enqueue group : %w", err)
		}
		msg.JobStatus[uid] = StatusStarted
	}

	if err := s.setGroupMessage(ctx, msg); err != nil {
		return "", err
	}
	return msg.ID, nil
}

func (s *Server) GetGroup(ctx context.Context, id string) (GroupMessage, error) {
	g, err := s.getGroupMessage(ctx, id)
	if err != nil {
		return g, err
	}
	// If the group status is either "done" or "failed".
	// Do an early return
	if g.Status == StatusDone || g.Status == StatusFailed {
		return g, nil
	}

	// jobStatus holds the updated map of job status'
	jobStatus := make(map[string]string)

	// Run over the individual jobs and their status.
	for id, status := range g.JobStatus {
		switch status {
		// Jobs with a final status remain the same and do not require lookup
		case StatusFailed, StatusDone:
			jobStatus[id] = status
		// Re-look the jobs where the status is an intermediatery state (processing, retrying, etc).
		case StatusStarted, StatusProcessing, StatusRetrying:
			j, err := s.GetJob(ctx, id)
			if err != nil {
				return GroupMessage{}, err
			}
			jobStatus[id] = j.Status

		}
	}

	// Update the overall group status, based on the individual jobs.
	g.JobStatus = jobStatus
	g.Status = getGroupStatus(jobStatus)

	// Re-set the group result in the store.
	if err := s.setGroupMessage(ctx, g); err != nil {
		return GroupMessage{}, err
	}

	return g, nil
}

func getGroupStatus(jobStatus map[string]string) string {
	status := StatusDone
	for _, st := range jobStatus {
		if st == StatusFailed {
			return StatusFailed
		}
		if st != StatusDone {
			status = StatusProcessing
		}
	}

	return status
}

const groupPrefix = "group:msg:"

func (s *Server) setGroupMessage(ctx context.Context, g GroupMessage) error {
	b, err := json.Marshal(g)
	if err != nil {
		return err
	}
	return s.results.Set(ctx, groupPrefix+g.ID, b)
}

func (s *Server) getGroupMessage(ctx context.Context, id string) (GroupMessage, error) {
	b, err := s.GetResult(ctx, groupPrefix+id)
	if err != nil {
		return GroupMessage{}, err
	}

	var g GroupMessage
	if err := json.Unmarshal(b, &g); err != nil {
		return GroupMessage{}, err
	}

	return g, nil
}
