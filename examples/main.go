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
	time.Sleep(time.Second * 10)

	return nil
}

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	srv := tasqueue.NewServer()
	srv.RegisterProcessor("add", SumProcessor)

	var tasks []*tasqueue.Task

	for i := 0; i < 3; i++ {
		b, _ := json.Marshal(SumPayload{Arg1: i, Arg2: 4})
		task, err := tasqueue.NewTask("add", b)
		if err != nil {
			log.Fatal(err)
		}
		tasks = append(tasks, task)
	}

	t, _ := tasqueue.NewChain(tasks...)
	srv.AddTask(ctx, t)

	srv.Start(ctx, tasqueue.Concurrency(5))

	// Create a task payload.
	fmt.Println("exit..")
}
