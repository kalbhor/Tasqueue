package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/kalbhor/tasqueue/v2"
	rb "github.com/kalbhor/tasqueue/v2/brokers/redis"
	"github.com/kalbhor/tasqueue/v2/examples/tasks"
	rr "github.com/kalbhor/tasqueue/v2/results/redis"
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
			Addrs:      []string{"127.0.0.1:6379"},
			Password:   "",
			DB:         0,
			MetaExpiry: time.Second * 5,
		}, lo),
		Logger: lo,
	})
	if err != nil {
		log.Fatal(err)
	}

	err = srv.RegisterTask("add", tasks.SumProcessor, tasqueue.TaskOpts{Concurrency: 5})
	if err != nil {
		log.Fatal(err)
	}

	go srv.Start(ctx)

	b, _ := json.Marshal(tasks.SumPayload{Arg1: 5, Arg2: 4})
	task, err := tasqueue.NewJob("add", b, tasqueue.JobOpts{})
	if err != nil {
		log.Fatal(err)
	}
	srv.Enqueue(ctx, task)

	b, _ = json.Marshal(tasks.SumPayload{Arg1: 5, Arg2: 4})
	task, err = tasqueue.NewJob("add", b, tasqueue.JobOpts{})
	if err != nil {
		log.Fatal(err)
	}
	srv.Enqueue(ctx, task)
	fmt.Println("exit..")
	for {
		select {
		case <-time.NewTicker(time.Second * 1).C:
			ids, err := srv.GetSuccess(ctx)
			if err != nil {
				log.Fatal(err)
			}

			log.Println(ids)
		}
	}

	// Create a task payload.
	fmt.Println("exit..")
}
