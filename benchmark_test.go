package tasqueue

import (
	"context"
	"log"
	"log/slog"
	"sync"
	"testing"

	rb "github.com/kalbhor/tasqueue/v2/brokers/redis"
	rr "github.com/kalbhor/tasqueue/v2/results/redis"
	"github.com/redis/go-redis/v9"
)

// The benchmarks use redis as results & broker.
const (
	redisAddr = "127.0.0.1:6379"
	redisPass = ""
	redisDB   = 0

	sampleHandler = "sample-handler"
)

// newJob returns a job configured for the sample handler with an empty byte payload.
func newJob(b *testing.B) Job {
	job, err := NewJob(sampleHandler, []byte{}, JobOpts{})
	if err != nil {
		b.Fatal(err)
	}
	return job
}

// serverWithRedis returns a tasqueue server with redis as broker and results.
func serverWithRedis(b *testing.B) *Server {
	lo := slog.Default()
	srv, err := NewServer(ServerOpts{
		Broker: rb.New(rb.Options{
			Addrs:    []string{redisAddr},
			Password: redisPass,
			DB:       redisDB,
		}, lo),
		Results: rr.New(rr.Options{
			Addrs:    []string{redisAddr},
			Password: redisPass,
			DB:       redisDB,
		}, lo),
		Logger: lo.Handler(),
	})
	if err != nil {
		b.Fatal(err)
	}

	return srv
}

func flushRedis() {
	conn := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:    []string{redisAddr},
		DB:       redisDB,
		Password: redisPass,
	})
	conn.FlushDB(context.Background())
	conn.Close()
}

// BenchmarkJob benchmarks processing 100000 jobs.
// 10 workers are configured to consume the jobs concurrently. The benchmark starts AFTER
// enqueing all the jobs on the server.
func BenchmarkJob(b *testing.B) {
	const (
		num         = 100000
		concurrency = 10
	)

	flushRedis()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		ctx, cancel := context.WithCancel(context.Background())
		// Create the server
		srv := serverWithRedis(b)

		// Waitgroup to indicate when jobs finish
		var wg sync.WaitGroup
		wg.Add(num)
		// handler is essentially an empty job handler.
		handler := func(b []byte, ctx JobCtx) error {
			wg.Done()
			return nil
		}

		// Register the handler and enqueue the jobs.
		err := srv.RegisterTask(sampleHandler, handler, TaskOpts{Concurrency: concurrency})
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < num; i++ {
			if _, err := srv.Enqueue(ctx, newJob(b)); err != nil {
				b.Fatalf("could not enqueue job : %v", err)
			}
		}

		b.StartTimer()
		go srv.Start(ctx)

		// Wait for jobs to complete & stop the timer.
		wg.Wait()
		b.StopTimer()

		// Cancel the context so that the server can shutdown.
		cancel()
		b.StartTimer()
	}
}

// BenchmarkGroup benchmarks processing 10000 chains with 10 jobs each.
// 10 workers are configured to consume the groups concurrently. The benchmark starts AFTER
// enqueing all the chains on the server.
func BenchmarkChain(b *testing.B) {
	const (
		num         = 10000
		jobNum      = 10
		concurrency = 10
	)

	flushRedis()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		ctx, cancel := context.WithCancel(context.Background())
		// Create the server
		srv := serverWithRedis(b)

		// Waitgroup to indicate when jobs finish
		var wg sync.WaitGroup
		wg.Add(num * jobNum)

		// handler is essentially an empty job handler.
		handler := func(x []byte, ctx JobCtx) error {
			wg.Done()
			return nil
		}

		// Register the handler and enqueue the jobs.
		err := srv.RegisterTask(sampleHandler, handler, TaskOpts{Concurrency: concurrency})
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < num; i++ {
			// Create a list of jobs to generate a chain.
			var jobs = make([]Job, jobNum)
			for j := 0; j < jobNum; j++ {
				jobs[j] = newJob(b)
			}

			chain, err := NewChain(jobs, ChainOpts{})
			if err != nil {
				log.Fatal(err)
			}

			if _, err := srv.EnqueueChain(ctx, chain); err != nil {
				b.Fatalf("could not enqueue chain : %v", err)
			}
		}

		b.StartTimer()
		go srv.Start(ctx)

		// Wait for jobs to complete & stop the timer.
		wg.Wait()
		b.StopTimer()

		// Cancel the context so that the server can shutdown.
		cancel()
		b.StartTimer()
	}
}

// BenchmarkGroup benchmarks processing 10000 groups with 10 jobs each.
// 10 workers are configured to consume the groups concurrently. The benchmark starts AFTER
// enqueing all the groups on the server.
func BenchmarkGroup(b *testing.B) {
	const (
		num         = 10000
		jobNum      = 10
		concurrency = 10
	)

	flushRedis()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		b.StopTimer()
		ctx, cancel := context.WithCancel(context.Background())
		// Create the server
		srv := serverWithRedis(b)

		// Waitgroup to indicate when jobs finish
		var wg sync.WaitGroup
		wg.Add(num * jobNum)

		// handler is essentially an empty job handler.
		handler := func(x []byte, ctx JobCtx) error {
			wg.Done()
			return nil
		}

		// Register the handler and enqueue the jobs.
		err := srv.RegisterTask(sampleHandler, handler, TaskOpts{Concurrency: concurrency})
		if err != nil {
			b.Fatal(err)
		}
		for i := 0; i < num; i++ {
			// Create a list of jobs to generate a group.
			var jobs = make([]Job, jobNum)
			for j := 0; j < jobNum; j++ {
				jobs[j] = newJob(b)
			}

			group, err := NewGroup(jobs, GroupOpts{})
			if err != nil {
				b.Fatal(err.Error())
			}

			if _, err := srv.EnqueueGroup(ctx, group); err != nil {
				b.Fatalf("could not enqueue group : %v", err)
			}
		}

		b.StartTimer()
		go srv.Start(ctx)

		// Wait for jobs to complete & stop the timer.
		wg.Wait()
		b.StopTimer()

		// Cancel the context so that the server can shutdown.
		cancel()
		b.StartTimer()
	}
}
