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
	srv.RegisterTask("validate-save", handler, TaskOpts{})

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
