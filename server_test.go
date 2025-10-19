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

// TestChainErrorHandling verifies that GetChain returns proper errors with context
func TestChainErrorHandling(t *testing.T) {
	t.Log("=== Testing Chain Error Handling ===")
	
	srv := newServer(t, "test", func(b []byte, ctx JobCtx) error {
		return nil
	})

	// Test 1: Get non-existent chain
	_, err := srv.GetChain(context.Background(), "non-existent-chain")
	if err == nil {
		t.Fatal("Expected error for non-existent chain, got nil")
	}
	t.Logf("✅ Non-existent chain error: %v", err)

	// Test 2: Create a chain and verify normal operation first
	job1, _ := NewJob("test", []byte("job1"), JobOpts{})
	job2, _ := NewJob("test", []byte("job2"), JobOpts{})
	chain, err := NewChain([]Job{job1, job2}, ChainOpts{})
	if err != nil {
		t.Fatal("Failed to create chain:", err)
	}

	chainID, err := srv.EnqueueChain(context.Background(), chain)
	if err != nil {
		t.Fatal("Failed to enqueue chain:", err)
	}
	t.Logf("Created chain: %s", chainID)

	// Test 3: Verify proper error context by testing with invalid job ID
	// (This simulates what happens when job storage is corrupted or jobs are deleted)
	chainMsg, err := srv.getChainMessage(context.Background(), chainID)
	if err != nil {
		t.Fatal("Failed to get chain message:", err)
	}

	// Set an invalid job ID to test error handling
	chainMsg.JobID = "definitely-non-existent-job-id-12345"
	err = srv.setChainMessage(context.Background(), chainMsg)
	if err != nil {
		t.Fatal("Failed to set modified chain message:", err)
	}

	// Try to get chain with invalid job reference - should get descriptive error
	_, err = srv.GetChain(context.Background(), chainID)
	if err == nil {
		t.Fatal("Expected error for invalid job reference, got nil")
	}
	
	// Verify error contains helpful context for debugging
	errorStr := err.Error()
	t.Logf("✅ Invalid job reference error: %v", err)
	
	// Check that error message contains key debugging information
	requiredContexts := []string{
		"failed to get current job",           // Action that failed
		"definitely-non-existent-job-id",     // The problematic job ID  
		chainID,                               // The chain being processed
	}
	
	for _, context := range requiredContexts {
		if !stringContains(errorStr, context) {
			t.Errorf("Error message missing expected context '%s' in: %s", context, errorStr)
		}
	}
	
	t.Log("✅ Error messages contain proper context for debugging")
}

// Helper function to check if string contains substring
func stringContains(s, substr string) bool {
	if len(substr) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			if s[i+j] != substr[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
