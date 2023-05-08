package tasqueue

import (
	"context"
	"testing"
	"time"
)

func TestEnqueueGroup(t *testing.T) {
	var (
		ctx   = context.Background()
		srv   = newServer(t)
		group = makeGroup(t, false, false)
	)
	go srv.Start(ctx)

	uuid, err := srv.EnqueueGroup(ctx, group)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Enqueued job with uuid : %s\n", uuid)
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
		time.Sleep(time.Second)
		msg, err := srv.GetGroup(ctx, uuid)
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
	grp, err := NewGroup(jobs...)
	if err != nil {
		t.Fatal(err)
	}

	return grp
}
