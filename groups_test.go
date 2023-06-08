package tasqueue

import (
	"context"
	"testing"
	"time"
)

func TestEnqueueGroup(t *testing.T) {
	var (
		ctx   = context.Background()
		srv   = newServer(t, taskName, MockHandler)
		group = makeGroup(t, false, false)
	)
	go srv.Start(ctx)

	id, err := srv.EnqueueGroup(ctx, group)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Enqueued job with id : %s\n", id)
}

func TestGetGroup(t *testing.T) {
	var (
		groups = map[string]Group{
			StatusDone:   makeGroup(t, false, false),
			StatusFailed: makeGroup(t, true, true),
		}
		srv = newServer(t, taskName, MockHandler)
		ctx = context.Background()
	)
	go srv.Start(ctx)

	for status, group := range groups {
		id, err := srv.EnqueueGroup(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		// Wait for task to be consumed & processed.
		time.Sleep(time.Second)
		msg, err := srv.GetGroup(ctx, id)
		if err != nil {
			t.Fatal(err)
		}

		if msg.Status != status {
			t.Fatalf("incorrect job status, expected %s, got %s", status, msg.Status)
		}
	}
}

func makeGroup(t *testing.T, fs ...bool) Group {
	var jobs []Job
	for _, f := range fs {
		jobs = append(jobs, makeJob(t, taskName, f))
	}
	grp, err := NewGroup(jobs, GroupOpts{})
	if err != nil {
		t.Fatal(err)
	}

	return grp
}
