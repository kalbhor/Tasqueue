package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/kalbhor/tasqueue/v2"
	sqb "github.com/kalbhor/tasqueue/v2/brokers/sqlite"
	"github.com/kalbhor/tasqueue/v2/examples/tasks"
	sqr "github.com/kalbhor/tasqueue/v2/results/sqlite"
)

func main() {
	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	lo := slog.Default()
	broker, err := sqb.New(sqb.Options{
		DataSource: "jobs.db",
	}, lo)
	if err != nil {
		log.Fatal(err)
	}
	results, err := sqr.New(sqr.Options{
		DataSource: "results",
	}, lo)
	if err != nil {
		log.Fatal(err)
	}
	srv, err := tasqueue.NewServer(tasqueue.ServerOpts{
		Broker:  broker,
		Results: results,
		Logger:  lo.Handler(),
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
}
