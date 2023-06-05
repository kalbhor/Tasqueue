package tasqueue

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/zerodha/logf"

	rb "github.com/kalbhor/tasqueue/brokers/in-memory"
	rr "github.com/kalbhor/tasqueue/results/in-memory"
)

const (
	taskName = "mock_handler"
)

func newServer(t *testing.T, taskName string, handler func([]byte, JobCtx) error) *Server {
	lo := logf.New(logf.Opts{
		Level: logf.DebugLevel,
	})
	srv, err := NewServer(ServerOpts{
		Broker:  rb.New(),
		Results: rr.New(),
		Logger:  lo,
	})
	if err != nil {
		t.Fatal(err)
	}
	srv.RegisterTask(taskName, handler, TaskOpts{
		Concurrency: 5,
	})

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
