package tasqueue

import (
	"context"
	"testing"
	"time"
)

func TestEnqueueChain(t *testing.T) {
	var (
		ctx   = context.Background()
		srv   = newServer(t)
		chain = makeChain(t, false, false)
	)
	go srv.Start(ctx)

	uuid, err := srv.EnqueueChain(ctx, chain)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Enqueued chain with uuid : %s\n", uuid)
}

func TestGetChain(t *testing.T) {
	var (
		chains = map[string]Chain{
			StatusDone:   makeChain(t, false, false),
			StatusFailed: makeChain(t, false, true),
		}
		srv = newServer(t)
		ctx = context.Background()
	)
	go srv.Start(ctx)

	for status, chain := range chains {
		uuid, err := srv.EnqueueChain(ctx, chain)
		if err != nil {
			t.Fatal(err)
		}
		// Wait for jobs to be consumed & processed.
		time.Sleep(time.Second)
		msg, err := srv.GetChain(ctx, uuid)
		if err != nil {
			t.Fatal(err)
		}

		if msg.Status != status {
			t.Fatalf("incorrect job status, expected %s, got %s", status, msg.Status)
		}
	}
}

func makeChain(t *testing.T, fs ...bool) Chain {
	var jobs []Job
	for _, f := range fs {
		jobs = append(jobs, makeJob(t, f))
	}
	chn, err := NewChain(jobs...)
	if err != nil {
		t.Fatal(err)
	}

	return chn
}
