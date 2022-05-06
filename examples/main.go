package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	tasqueue "go.zerodha.tech/kalbhor/kronika"
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
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	srv := tasqueue.NewServer()
	srv.RegisterProcessor("add", SumProcessor)

	go func() {
		for i := 0; i < 1000; i++ {
			b, _ := json.Marshal(SumPayload{Arg1: i, Arg2: 4})
			task, err := tasqueue.NewTask("add", b)
			if err != nil {
				log.Fatal(err)
			}
			if err := srv.AddTask(ctx, task); err != nil {
				log.Fatal(err)
			}
			time.Sleep(time.Second)
		}
	}()

	srv.Start(ctx, tasqueue.Concurrency(5))

	// Create a task payload.
	fmt.Println("exit..")
}
