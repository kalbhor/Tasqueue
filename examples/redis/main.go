package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/kalbhor/tasqueue"
	rbroker "github.com/kalbhor/tasqueue/brokers/redis"
	"github.com/kalbhor/tasqueue/examples/tasks"
	rresults "github.com/kalbhor/tasqueue/results/redis"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	srv := tasqueue.NewServer(rbroker.New(rbroker.Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}), rresults.New(rresults.Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}))

	srv.RegisterProcessor("add", tasks.SumProcessor)

	var chain []*tasqueue.Task

	for i := 0; i < 3; i++ {
		b, _ := json.Marshal(tasks.SumPayload{Arg1: i, Arg2: 4})
		task, err := tasqueue.NewTask("add", b)
		if err != nil {
			log.Fatal(err)
		}
		chain = append(chain, task)
	}

	t, _ := tasqueue.NewChain(chain...)
	srv.AddTask(ctx, t)

	srv.Start(ctx, tasqueue.Concurrency(5))

	// Create a task payload.
	fmt.Println("exit..")
}
