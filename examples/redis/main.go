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
	rb "github.com/kalbhor/tasqueue/brokers/redis"
	"github.com/kalbhor/tasqueue/examples/tasks"
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

	srv.RegisterTask("add", tasks.SumProcessor, tasqueue.TaskOpts{
		Concurrency: 5,
	})

	var group []tasqueue.Job

	for i := 0; i < 3; i++ {
		b, _ := json.Marshal(tasks.SumPayload{Arg1: i, Arg2: 4})
		task, err := tasqueue.NewJob("add", b, tasqueue.JobOpts{})
		if err != nil {
			log.Fatal(err)
		}
		group = append(group, task)
	}

	t, _ := tasqueue.NewGroup(group, tasqueue.GroupOpts{})
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
