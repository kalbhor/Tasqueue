package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/kalbhor/tasqueue"
	rb "github.com/kalbhor/tasqueue/brokers/in-memory"
	"github.com/kalbhor/tasqueue/examples/tasks"
	rr "github.com/kalbhor/tasqueue/results/in-memory"
	"github.com/zerodha/logf"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

// newExporter returns a console exporter.
func newExporter(w io.Writer) (trace.SpanExporter, error) {
	return stdouttrace.New(
		stdouttrace.WithWriter(w),
		// Use human-readable output.
		stdouttrace.WithPrettyPrint(),
		// Do not print timestamps for the demo.
		stdouttrace.WithoutTimestamps(),
	)
}

// newResource returns a resource describing this application.
func newResource() *resource.Resource {
	r, _ := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("tasqueue"),
			semconv.ServiceVersionKey.String("v0.1.0"),
			attribute.String("environment", "demo"),
		),
	)
	return r
}

func main() {
	l := log.New(os.Stdout, "", 0)

	// Write telemetry data to a file.
	f, err := os.Create("traces.txt")
	if err != nil {
		l.Fatal(err)
	}
	defer f.Close()

	exp, err := newExporter(f)
	if err != nil {
		l.Fatal(err)
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exp),
		trace.WithResource(newResource()),
	)
	defer func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			l.Fatal(err)
		}
	}()
	otel.SetTracerProvider(tp)

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	srv, err := tasqueue.NewServer(tasqueue.ServerOpts{
		Broker:        rb.New(),
		Results:       rr.New(),
		Logger:        logf.New(logf.Opts{}),
		TraceProvider: tp,
	})
	if err != nil {
		log.Fatal(err)
	}

	srv.RegisterTask("add", tasks.SumProcessor, tasqueue.TaskOpts{})

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
