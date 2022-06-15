package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/kalbhor/tasqueue"
	rb "github.com/kalbhor/tasqueue/brokers/redis"
	"github.com/kalbhor/tasqueue/examples/tasks"
	rr "github.com/kalbhor/tasqueue/results/redis"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	srv, err := tasqueue.NewServer(rb.New(rb.Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}), rr.New(rr.Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}))
	if err != nil {
		log.Fatal(err)
	}

	srv.RegisterTask("add", tasks.SumProcessor, tasqueue.TaskOpts{
		Concurrency: 10,
	})

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		srv.Start(ctx)
		wg.Done()
	}()
Loop:
	for {
		select {
		case <-time.Tick(1 * time.Second):
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
			srv.EnqueueGroup(ctx, t)
		case <-ctx.Done():
			break Loop
		}
	}

	wg.Wait()

	// Create a task payload.
	fmt.Println("exit..")
}
