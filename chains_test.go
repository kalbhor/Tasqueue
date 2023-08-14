package tasqueue

import (
	"context"
	"testing"
	"time"
)

const chainTask1 = "chain-task-1"
const chainTask = "chain-task"

func TestEnqueueChain(t *testing.T) {
	var (
		ctx   = context.Background()
		srv   = newServer(t, taskName, MockHandler)
		chain = makeChain(t, taskName, false, false)
	)
	go srv.Start(ctx)

	id, err := srv.EnqueueChain(ctx, chain)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Enqueued chain with id : %s\n", id)
}

func TestGetChain(t *testing.T) {
	var (
		// initHandler saves data for the next job in the chain
		initHandler = func(d []byte, j JobCtx) error {
			if err := j.Save(d); err != nil {
				t.Fatal(err)
			}

			return nil
		}
		// handler hecks for the previous jobs data in the chain
		handler = func(d []byte, j JobCtx) error {
			if string(j.Meta.PrevJobResult) != string(d) {
				t.Fatalf("chain previous results do not match. got=%v, want=%v", string(j.Meta.PrevJobResult), string(d))
			}

			return nil
		}
		srv = newServer(t, taskName, MockHandler)
		ctx = context.Background()
	)

	err := srv.RegisterTask(chainTask1, initHandler, TaskOpts{})
	if err != nil {
		t.Fatal(err)
	}
	err = srv.RegisterTask(chainTask, handler, TaskOpts{})
	if err != nil {
		t.Fatal(err)
	}

	// Create sequential list of jobs for the chain.
	var jobs = []Job{
		makeJob(t, chainTask1, false),
		makeJob(t, chainTask, false),
	}

	chain, err := NewChain(jobs, ChainOpts{})
	if err != nil {
		t.Fatal(err)
	}

	go srv.Start(ctx)

	id, err := srv.EnqueueChain(ctx, chain)
	if err != nil {
		t.Fatal(err)
	}
	// Wait for jobs to be consumed & processed.
	time.Sleep(time.Second * 1)
	msg, err := srv.GetChain(ctx, id)
	if err != nil {
		t.Fatal(err)
	}

	if msg.Status != StatusDone {
		t.Fatalf("incorrect job status, expected %s, got %s", StatusDone, msg.Status)
	}
}

func makeChain(t *testing.T, taskName string, fs ...bool) Chain {
	var jobs []Job
	for _, f := range fs {
		jobs = append(jobs, makeJob(t, taskName, f))
	}
	chn, err := NewChain(jobs, ChainOpts{})
	if err != nil {
		t.Fatal(err)
	}

	return chn
}
