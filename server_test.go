package tasqueue

import (
	"context"
	"testing"
)

func TestEnqueue(t *testing.T) {
	srv, err := NewServer(NewMockBroker(), NewMockResults())
	if err != nil {
		t.Fatal(err)
	}
	srv.RegisterTask("mock_handler", MockHandler)

	job := makeJob(false)

	uuid, err := srv.Enqueue(context.Background(), job)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Enqueued job with uuid : %s\n", uuid)
}

func TestEnqueueGroup(t *testing.T) {
	srv, err := NewServer(NewMockBroker(), NewMockResults())
	if err != nil {
		t.Fatal(err)
	}
	srv.RegisterTask("mock_handler", MockHandler)

	group := makeGroup(false, false)

	uuid, err := srv.EnqueueGroup(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Enqueued job with uuid : %s\n", uuid)
}

func TestGetGroup(t *testing.T) {
	jobs := map[string]*Job{
		StatusDone:   makeJob(false),
		StatusFailed: makeJob(true),
	}
	srv, err := NewServer(NewMockBroker(), NewMockResults())
	if err != nil {
		t.Fatal(err)
	}
	srv.RegisterTask("mock_handler", MockHandler)
	go srv.Start(context.Background())

	for k, job := range jobs {

		uuid, err := srv.Enqueue(context.Background(), job)
		if err != nil {
			t.Fatal(err)
		}

		msg, err := srv.GetJob(context.Background(), uuid)
		if err != nil {
			t.Fatal(err)
		}

		// if err := srv.execJob(context.Background(), msg, MockHandler); err != nil {
		// 	t.Fatal(err)
		// }

		// msg, err = srv.GetJob(context.Background(), uuid)
		// if err != nil {
		// 	t.Fatal(err)
		// }

		switch k {
		case StatusDone:
			if msg.Status != StatusDone {
				t.Fatalf("incorrect job status, expected %s, got %s", StatusDone, msg.Status)
			}
		case StatusFailed:
			if msg.Status != StatusFailed {
				t.Fatalf("incorrect job status, expected %s, got %s", StatusFailed, msg.Status)
			}
		}
	}
}

func TestExecJob(t *testing.T) {
	jobs := map[string]*Job{
		StatusDone:   makeJob(false),
		StatusFailed: makeJob(true),
	}
	srv, err := NewServer(NewMockBroker(), NewMockResults())
	if err != nil {
		t.Fatal(err)
	}
	srv.RegisterTask("mock_handler", MockHandler)
	go srv.Start(context.Background())

	for k, job := range jobs {

		uuid, err := srv.Enqueue(context.Background(), job)
		if err != nil {
			t.Fatal(err)
		}

		msg, err := srv.GetJob(context.Background(), uuid)
		if err != nil {
			t.Fatal(err)
		}

		// if err := srv.execJob(context.Background(), msg, MockHandler); err != nil {
		// 	t.Fatal(err)
		// }

		// msg, err = srv.GetJob(context.Background(), uuid)
		// if err != nil {
		// 	t.Fatal(err)
		// }

		switch k {
		case StatusDone:
			if msg.Status != StatusDone {
				t.Fatalf("incorrect job status, expected %s, got %s", StatusDone, msg.Status)
			}
		case StatusFailed:
			if msg.Status != StatusFailed {
				t.Fatalf("incorrect job status, expected %s, got %s", StatusFailed, msg.Status)
			}
		}
	}
}
