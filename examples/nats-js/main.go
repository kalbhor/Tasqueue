package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/kalbhor/tasqueue/v2"
	nats_broker "github.com/kalbhor/tasqueue/v2/brokers/nats-js"
	"github.com/kalbhor/tasqueue/v2/examples/tasks"
	nats_result "github.com/kalbhor/tasqueue/v2/results/nats-js"
	"github.com/zerodha/logf"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	lo := logf.New(logf.Opts{})
	brkr, err := nats_broker.New(nats_broker.Options{
		URL:         "localhost:4222",
		EnabledAuth: false,
		Streams: map[string][]string{
			"default": {tasqueue.DefaultQueue},
		},
	}, lo)
	if err != nil {
		log.Fatal(err)
	}

	res, err := nats_result.New(nats_result.Options{
		URL:         "localhost:4222",
		EnabledAuth: false,
	}, lo)
	if err != nil {
		log.Fatal(err)
	}

	srv, err := tasqueue.NewServer(tasqueue.ServerOpts{
		Broker:  brkr,
		Results: res,
		Logger:  logf.New(logf.Opts{}),
	})
	if err != nil {
		log.Fatal(err)
	}

	err = srv.RegisterTask("add", tasks.SumProcessor, tasqueue.TaskOpts{})
	if err != nil {
		log.Fatal(err)
	}

	var chain []tasqueue.Job

	for i := 0; i < 3; i++ {
		b, _ := json.Marshal(tasks.SumPayload{Arg1: i, Arg2: 4})
		task, err := tasqueue.NewJob("add", b, tasqueue.JobOpts{})
		if err != nil {
			log.Fatal(err)
		}
		chain = append(chain, task)
	}

	t, _ := tasqueue.NewChain(chain, tasqueue.ChainOpts{})
	srv.EnqueueChain(ctx, t)

	srv.Start(ctx)

	// Create a task payload.
	fmt.Println("exit..")
}
