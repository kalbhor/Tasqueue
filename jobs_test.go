package tasqueue

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestEnqueue(t *testing.T) {
	var (
		ctx = context.Background()
		srv = newServer(t, taskName, MockHandler)
		job = makeJob(t, taskName, false)
	)
	go srv.Start(ctx)
	uuid, err := srv.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Enqueued job with uuid : %s\n", uuid)
}

func TestJobWithTimeout(t *testing.T) {
	var (
		ctx = context.Background()
		srv = newServer(t, "mock_timeout", MockHandlerWithSleep)
		job = makeJob(t, "mock_timeout", false)
	)
	go srv.Start(ctx)

	job.Opts.Timeout = 1 * time.Second
	job.Opts.MaxRetries = 0

	uuid, err := srv.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for task to be consumed & processed.
	time.Sleep(3 * time.Second)

	// Check if the job has been marked as failed.
	msg, err := srv.GetJob(ctx, uuid)
	if err != nil {
		t.Fatal(err)
	}

	if !(msg.Status == StatusFailed) {
		t.Fatalf("incorrect job status, expected %s, got %s", StatusFailed, msg.Status)
	}
}

func TestGetPending(t *testing.T) {
	var (
		jobs = map[string]Job{
			StatusDone:   makeJob(t, taskName, false),
			StatusFailed: makeJob(t, taskName, true),
		}
		srv = newServer(t, taskName, MockHandler)
		ctx = context.Background()
	)

	// Enqueue the jobs
	for _, job := range jobs {
		_, err := srv.Enqueue(ctx, job)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Check the queue for pending
	msg, err := srv.GetPending(ctx, DefaultQueue)
	if err != nil {
		t.Fatal(err)
	}

	// Pending jobs must be equal to enqueued as of now
	if len(msg) != len(jobs) {
		t.Fatalf("pending job not found. resp: %+v, queued:%d", msg, len(jobs))
	}

	// Start consuming jobs
	go srv.Start(ctx)
	time.Sleep(2 * time.Second)

	// Get pending (again) and check if jobs have been consumed
	msg, err = srv.GetPending(ctx, DefaultQueue)
	if err != nil {
		t.Fatal(err)
	}

	if len(msg) != 0 {
		t.Fatalf("pending job not found. resp: %+v", msg)
	}

}

func TestGetJob(t *testing.T) {
	var (
		jobs = map[string]Job{
			StatusDone:   makeJob(t, taskName, false),
			StatusFailed: makeJob(t, taskName, true),
		}
		srv = newServer(t, taskName, MockHandler)
		ctx = context.Background()
	)
	go srv.Start(ctx)

	for status, job := range jobs {
		uuid, err := srv.Enqueue(ctx, job)
		if err != nil {
			t.Fatal(err)
		}

		// Wait for task to be consumed & processed.
		time.Sleep(time.Second * 2)
		msg, err := srv.GetJob(ctx, uuid)
		if err != nil {
			t.Fatal(err)
		}

		if msg.Status != status {
			t.Fatalf("incorrect job status, expected %s, got %s", status, msg.Status)
		}
	}
}

func TestSaveJob(t *testing.T) {
	var (
		// handler simply saves the passed data onto the job's results
		handler = func(d []byte, j JobCtx) error {
			if err := j.Save(d); err != nil {
				t.Fatal(err)
			}

			return nil
		}
		savedData = "saved results"
		srv       = newServer(t, taskName, MockHandler)
		ctx       = context.Background()
	)

	// Register the task and handler
	err := srv.RegisterTask("validate-save", handler, TaskOpts{})
	if err != nil {
		t.Fatal(err)
	}

	// Create a job that passes the data needed to be saved.
	job, err := NewJob("validate-save", []byte(savedData), JobOpts{MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}

	go srv.Start(ctx)

	uuid, err := srv.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for task to be consumed & processed.
	time.Sleep(time.Second)

	results, err := srv.GetResult(ctx, uuid)
	if err != nil {
		t.Fatal(err)
	}

	if string(results) != savedData {
		t.Fatalf("saved results don't match results fetched.\nsaved:%v\nfetched:%v", savedData, results)
	}

}

func TestDeleteJob(t *testing.T) {
	var (
		// handler simply saves the passed data onto the job's results
		handler = func(d []byte, j JobCtx) error {
			if err := j.Save(d); err != nil {
				t.Fatal(err)
			}

			return nil
		}
		savedData = "saved results"
		srv       = newServer(t, taskName, MockHandler)
		ctx       = context.Background()
	)

	// Register the task and handler
	err := srv.RegisterTask("validate-save", handler, TaskOpts{})
	if err != nil {
		t.Fatal(err)
	}

	// Create a job that passes the data needed to be saved.
	job, err := NewJob("validate-save", []byte(savedData), JobOpts{MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}

	go srv.Start(ctx)

	uuid, err := srv.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for task to be consumed & processed.
	time.Sleep(time.Second * 3)

	results, err := srv.GetResult(ctx, uuid)
	if err != nil {
		t.Fatal(err)
	}

	if string(results) != savedData {
		t.Fatalf("saved results don't match results fetched.\nsaved:%v\nfetched:%v", savedData, string(results))
	}

	if err := srv.DeleteJob(ctx, uuid); err != nil {
		t.Fatal(err)
	}

	_, err = srv.GetResult(ctx, uuid)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("job results not deleted: %v", err)
	}

}

func makeJob(t *testing.T, taskName string, doErr bool) Job {
	j, err := json.Marshal(MockPayload{ShouldErr: doErr})
	if err != nil {
		t.Fatal(err)
	}

	job, err := NewJob(taskName, j, JobOpts{MaxRetries: 1})
	if err != nil {
		t.Fatal(err)
	}

	return job
}
