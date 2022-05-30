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
	redis_broker "github.com/kalbhor/tasqueue/brokers/redis"
	"github.com/kalbhor/tasqueue/examples/tasks"
	redis_results "github.com/kalbhor/tasqueue/results/redis"
)

func successCB(j tasqueue.JobCtx) {
	log.Println("SUCCESS")
}

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	srv, err := tasqueue.NewServer(redis_broker.New(redis_broker.Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}), redis_results.New(redis_results.Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}), tasqueue.Concurrency(5))
	if err != nil {
		log.Fatal(err)
	}

	srv.RegisterTask("add", tasks.SumProcessor, tasqueue.SuccessCallback(successCB))

	var chain []tasqueue.Job

	for i := 0; i < 3; i++ {
		b, _ := json.Marshal(tasks.SumPayload{Arg1: i, Arg2: 4})
		task, err := tasqueue.NewJob("add", b)
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
