# Note : Work in progress, the docs may not to be accurate 

# Tasqueue
Tasqueue is a simple, lightweight distributed job/worker implementation in Go

## Concepts 
- `tasqueue.Broker` is a generic interface that provides methods to enqueue and consume messages from a queue. Currently supported brokers are
[redis](./brokers/redis/) and [nats-jetstream](./brokers/nats-js/).
- `tasqueue.Results` is a generic interface that provides methods to store the state/results of task messages. Currently supported result stores are
[redis](./results/redis/) and [nats-jetstream](./results/nats-js/).
- `tasqueue.Handler` is a function type that accepts `[]byte` payloads. Users need to register such handlers with the server. It is upto the handler to decode (if required) the `[]byte` messages and process them in any manner.
- `tasqueue.Task` holds the data for a basic unit of work. This data includes the handler name which will process the task, a `[]byte` payload (encoded in any manner, if required). More options described below.

## Basic example 
```go
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
	redis_results "github.com/kalbhor/tasqueue/results/redis"
)

type SumPayload struct {
	Arg1 int `json:"arg1"`
	Arg2 int `json:"arg2"`
}

// SumProcessor prints the sum of two integer arguements.
func SumProcessor(b []byte) error {
	var pl SumPayload
	if err := json.Unmarshal(b, &pl); err != nil {
		return err
	}

	fmt.Println(pl.Arg1 + pl.Arg2)

	return nil
}

func main() {
	// Create a new tasqueue server with redis results & broker.
	srv := tasqueue.NewServer(redis_broker.New(redis_broker.Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}), redis_results.New(redis_results.Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}))

	// Register a handler called "add"
	srv.RegisterHandler("add", SumProcessor)

	// Encode the payload passed to the handler and create a task.
	b, _ := json.Marshal(SumPayload{Arg1: 5, Arg2: 4})
	t, err := tasqueue.NewTask("add", b)
	if err != nil {
		log.Fatal(err)
	}

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)

	// Place the task
	srv.AddTask(ctx, t)

	// Start the tasqueue workers. (blocking function)
	srv.Start(ctx)

	fmt.Println("exit..")
}
```

## Features and Options 

### Task

#### Features
- Task chains : A new "chain" of tasks can be formed by using `tasqueue.NewChain(tasks ...*Task)`. Each subsequent task will be placed after the successful execution of current task.

#### Options
- Cron based schedule : `tasqueue.Schedule("* * * * *")`
- Custom Queue (important to run server on custom queue as well) : `tasqueue.CustomQueue("custom-q")`
- Custom value for maximum retries : `tasqueue.MaxRetry(5)`

Options can be passed while creating a new task : `func NewTask(handler string, payload []byte, opts ...Opts)`

### Server/Worker 
#### Options
- Custom Queue (important to set on task as well) : `tasqueue.CustomQueue("custom-q")`
- Custom concurrency : `tasqueue.Concurrency(5)`

Options can be passed while starting the server worker. 
`func (s *Server) Start(ctx context.Context, opts ...Opts)` 
