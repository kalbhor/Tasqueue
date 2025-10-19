package tasqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	rb "github.com/kalbhor/tasqueue/v2/brokers/in-memory"
	rr "github.com/kalbhor/tasqueue/v2/results/in-memory"
)

const (
	taskName = "mock_handler"
)

func newServer(t *testing.T, taskName string, handler func([]byte, JobCtx) error) *Server {
	lo := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))
	srv, err := NewServer(ServerOpts{
		Broker:  rb.New(),
		Results: rr.New(),
		Logger:  lo.Handler(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.RegisterTask(taskName, handler, TaskOpts{}); err != nil {
		t.Fatal(err)
	}

	return srv
}

type MockPayload struct {
	ShouldErr bool
}

func MockHandler(msg []byte, _ JobCtx) error {
	var m MockPayload
	if err := json.Unmarshal(msg, &m); err != nil {
		return err
	}

	if m.ShouldErr {
		return fmt.Errorf("task ended with error")
	}

	return nil
}

// MockHandlerWithSleep is a mock handler that sleeps for a long time.
func MockHandlerWithSleep(msg []byte, _ JobCtx) error {
	time.Sleep(3000 * time.Second)

	return nil
}

// TestCronLifecycleManagement verifies that cron scheduler starts and stops properly
func TestCronLifecycleManagement(t *testing.T) {
	// Create server with in-memory broker/results
	srv := newServer(t, "test", func(b []byte, ctx JobCtx) error {
		return nil
	})

	// Create a context that we can cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Start server in a goroutine
	done := make(chan bool)
	go func() {
		srv.Start(ctx)
		done <- true
	}()

	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)

	// Cancel the context to trigger shutdown
	cancel()

	// Wait for server to shut down with timeout
	select {
	case <-done:
		t.Log("Server shut down gracefully")
	case <-time.After(10 * time.Second):
		t.Fatal("Server failed to shut down within timeout - cron leak likely exists")
	}
}

// TestCronContextCancellationBeforeStart verifies that cron handles early cancellation
func TestCronContextCancellationBeforeStart(t *testing.T) {
	srv := newServer(t, "test", func(b []byte, ctx JobCtx) error {
		return nil
	})

	// Create a context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Start server in a goroutine
	done := make(chan bool)
	go func() {
		srv.Start(ctx)
		done <- true
	}()

	// Wait for server to shut down with timeout
	select {
	case <-done:
		t.Log("Server handled pre-cancelled context gracefully")
	case <-time.After(5 * time.Second):
		t.Fatal("Server failed to handle pre-cancelled context")
	}
}
