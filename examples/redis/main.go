package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/kalbhor/tasqueue"
	redis_broker "github.com/kalbhor/tasqueue/brokers/redis"
	"github.com/kalbhor/tasqueue/examples/tasks"
	redis_results "github.com/kalbhor/tasqueue/results/redis"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	srv := tasqueue.NewServer(redis_broker.New(redis_broker.Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}), redis_results.New(redis_results.Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}))

	srv.RegisterTask("add", tasks.SumProcessor)

	// var chain []*tasqueue.Task

	// for i := 0; i < 3; i++ {
	// 	b, _ := json.Marshal(tasks.SumPayload{Arg1: i, Arg2: 4})
	// 	task, err := tasqueue.NewTask("add", b)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	chain = append(chain, task)
	// }

	// t, _ := tasqueue.NewChain(chain...)
	b, _ := json.Marshal(tasks.SumPayload{Arg1: 5, Arg2: 4})
	t, err := tasqueue.NewJob("add", b, tasqueue.Schedule("* * * * *"))
	if err != nil {
		log.Fatal(err)
	}
	srv.Enqueue(ctx, t)

	srv.Start(ctx, tasqueue.Concurrency(5))

	// Create a task payload.
	fmt.Println("exit..")
}
