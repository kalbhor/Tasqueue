package tasqueue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

type Group struct {
	Jobs []Job
}

// GroupMeta contains fields related to a group job. These are updated when a task is consumed.
type GroupMeta struct {
	UUID   string
	Status string
	// JobStatus is a map of job uuid -> status
	JobStatus map[string]string
}

// GroupMessage is a wrapper over Group, containing meta info such as status, uuid.
// A GroupMessage is stored in the results store.
type GroupMessage struct {
	GroupMeta
	Group *Group
}

// message() converts a group into a group message, ready to be enqueued/stored.
func (t *Group) message() GroupMessage {
	return GroupMessage{
		GroupMeta: GroupMeta{
			JobStatus: make(map[string]string),
			UUID:      uuid.NewString(),
			Status:    StatusProcessing,
		},
		Group: t,
	}
}

// NewGroup() accepts a list of jobs and creates a group.
func NewGroup(j ...Job) (Group, error) {
	if len(j) < 2 {
		return Group{}, fmt.Errorf("minimum 2 tasks required to form group")
	}

	return Group{
		Jobs: j,
	}, nil

}

// EnqueueGroup() accepts a group and returns the assigned UUID.
// The following steps take place:
// 1. Converts it into a group message, which assigns a UUID (among other meta info) to the group.
// 2. Sets the group status as "started" on the results store.
// 3. Loops over all jobs part of the group and enqueues the job each job.
// 4. The job status map is updated with the uuids of each enqueued job.
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
	return msg.UUID, nil
}

func (s *Server) GetGroup(ctx context.Context, uuid string) (GroupMessage, error) {
	g, err := s.getGroupMessage(ctx, uuid)
	if err != nil {
		return g, nil
	}
	// If the group status is either "done" or "failed".
	// Do an early return
	if g.Status == StatusDone || g.Status == StatusFailed {
		return g, nil
	}

	// jobStatus holds the updated map of job status'
	jobStatus := make(map[string]string)

	// Run over the individual jobs and their status.
	for uuid, status := range g.JobStatus {
		switch status {
		// Jobs with a final status remain the same and do not require lookup
		case StatusFailed, StatusDone:
			jobStatus[uuid] = status
		// Re-look the jobs where the status is an intermediatery state (processing, retrying, etc).
		case StatusStarted, StatusProcessing, StatusRetrying:
			j, err := s.GetJob(ctx, uuid)
			if err != nil {
				return GroupMessage{}, err
			}
			jobStatus[uuid] = j.Status

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

func (s *Server) setGroupMessage(ctx context.Context, g GroupMessage) error {
	b, err := json.Marshal(g)
	if err != nil {
		return err
	}
	if err := s.results.Set(ctx, g.UUID, b); err != nil {
		return err
	}

	return nil
}

func (s *Server) getGroupMessage(ctx context.Context, uuid string) (GroupMessage, error) {
	b, err := s.results.Get(ctx, uuid)
	if err != nil {
		return GroupMessage{}, err
	}

	var g GroupMessage
	if err := json.Unmarshal(b, &g); err != nil {
		return GroupMessage{}, err
	}

	return g, nil
}
