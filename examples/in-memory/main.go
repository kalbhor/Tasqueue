package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/kalbhor/tasqueue"
	rb "github.com/kalbhor/tasqueue/brokers/in-memory"
	"github.com/kalbhor/tasqueue/examples/tasks"
	rr "github.com/kalbhor/tasqueue/results/in-memory"
	"github.com/zerodha/logf"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	srv, err := tasqueue.NewServer(rb.New(), rr.New(), logf.New(logf.Opts{}))
	if err != nil {
		log.Fatal(err)
	}

	srv.RegisterTask("add", tasks.SumProcessor, tasqueue.TaskOpts{})

	var chain []tasqueue.Job

	for i := 0; i < 3; i++ {
		b, _ := json.Marshal(tasks.SumPayload{Arg1: i, Arg2: 4})
		task, err := tasqueue.NewJob("add", b, tasqueue.JobOpts{})
		if err != nil {
			log.Fatal(err)
		}
		chain = append(chain, task)
	}

	t, _ := tasqueue.NewGroup(chain...)
	x, _ := srv.EnqueueGroup(ctx, t)
	go func() {
		for {
			select {
			case <-time.Tick(time.Second * 1):
				fmt.Println(srv.GetGroup(ctx, x))
			}
		}
	}()
	srv.Start(ctx)

	// Create a task payload.
	fmt.Println("exit..")
}
