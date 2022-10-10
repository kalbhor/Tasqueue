package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/kalbhor/tasqueue"
	rb "github.com/kalbhor/tasqueue/brokers/redis"
	rr "github.com/kalbhor/tasqueue/results/redis"
	"github.com/zerodha/logf"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	lo := logf.New(logf.Opts{})
	srv, err := tasqueue.NewServer(tasqueue.ServerOpts{
		Broker: rb.New(rb.Options{
			Addrs:    []string{"127.0.0.1:6379"},
			Password: "",
			DB:       0,
		}, lo),
		Results: rr.New(rr.Options{
			Addrs:    []string{"127.0.0.1:6379"},
			Password: "",
			DB:       0,
		}, lo),
		Logger: lo,
	})
	if err != nil {
		log.Fatal(err)
	}

	srv.RegisterTask("add", SumProcessor, tasqueue.TaskOpts{
		Concurrency: 5,
	})

	var chain []tasqueue.Job

	for i := 0; i < 3; i++ {
		b, _ := json.Marshal(SumPayload{Arg1: i, Arg2: 4})
		task, err := tasqueue.NewJob("add", b, tasqueue.JobOpts{})
		if err != nil {
			log.Fatal(err)
		}
		chain = append(chain, task)
	}

	t, _ := tasqueue.NewChain(chain...)
	srv.EnqueueChain(ctx, t)
	srv.Start(ctx)

	// Create a task payload.
	fmt.Println("exit..")
}

type SumPayload struct {
	Arg1 int `json:"arg1"`
	Arg2 int `json:"arg2"`
}

type SumResult struct {
	Result int `json:"result"`
}

// SumProcessor prints the sum of two integer arguements.
func SumProcessor(b []byte, m *tasqueue.JobCtx) error {
	log.Println(len(m.Meta.PrevJobResults))
	var pl SumPayload
	if err := json.Unmarshal(b, &pl); err != nil {
		return err
	}

	rs, err := json.Marshal(SumResult{Result: pl.Arg1 + pl.Arg2})
	if err != nil {
		return err
	}

	m.Save(rs)

	return nil
}
