package tasqueue

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestEnqueue(t *testing.T) {
	var (
		ctx = context.Background()
		srv = newServer(t)
		job = makeJob(t, false)
	)
	go srv.Start(ctx)
	uuid, err := srv.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Enqueued job with uuid : %s\n", uuid)
}

func TestGetJob(t *testing.T) {
	var (
		jobs = map[string]Job{
			StatusDone:   makeJob(t, false),
			StatusFailed: makeJob(t, true),
		}
		srv = newServer(t)
		ctx = context.Background()
	)
	go srv.Start(ctx)

	for status, job := range jobs {
		uuid, err := srv.Enqueue(ctx, job)
		if err != nil {
			t.Fatal(err)
		}

		// Wait for task to be consumed & processed.
		time.Sleep(time.Second)
		msg, err := srv.GetJob(ctx, uuid)
		if err != nil {
			t.Fatal(err)
		}

		if msg.Status != status {
			t.Fatalf("incorrect job status, expected %s, got %s", status, msg.Status)
		}
	}
}

func makeJob(t *testing.T, f bool) Job {
	j, err := json.Marshal(MockPayload{ShouldErr: f})
	if err != nil {
		t.Fatal(err)
	}

	job, err := NewJob(taskName, j, MaxRetry(1))
	if err != nil {
		t.Fatal(err)
	}

	return job
}
