package tasqueue

import (
	"context"
	"testing"
	"time"
)

func TestEnqueue(t *testing.T) {
	var (
		ctx = context.Background()
		srv = newServer(t)
		job = makeJob(t, false)
	)

	uuid, err := srv.Enqueue(ctx, job)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Enqueued job with uuid : %s\n", uuid)
}

func TestEnqueueGroup(t *testing.T) {
	var (
		ctx   = context.Background()
		srv   = newServer(t)
		group = makeGroup(t, false, false)
	)

	uuid, err := srv.EnqueueGroup(ctx, group)
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
		time.Sleep(3 * time.Second)
		msg, err := srv.GetJob(ctx, uuid)
		if err != nil {
			t.Fatal(err)
		}

		if msg.Status != status {
			t.Fatalf("incorrect job status, expected %s, got %s", status, msg.Status)
		}
	}
}

func TestGetGroup(t *testing.T) {
	var (
		groups = map[string]Group{
			StatusDone:   makeGroup(t, false, false),
			StatusFailed: makeGroup(t, true, true),
		}
		srv = newServer(t)
		ctx = context.Background()
	)
	go srv.Start(ctx)

	for status, group := range groups {
		uuid, err := srv.EnqueueGroup(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		// Wait for task to be consumed & processed.
		time.Sleep(3 * time.Second)
		msg, err := srv.GetGroup(ctx, uuid)
		if err != nil {
			t.Fatal(err)
		}

		if msg.Status != status {
			t.Fatalf("incorrect job status, expected %s, got %s", status, msg.Status)
		}
	}
}
