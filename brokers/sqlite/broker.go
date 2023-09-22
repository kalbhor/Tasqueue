package sqlite

import (
	"context"
	"fmt"
	"time"
)

type Broker struct{}

func (broker *Broker) Enqueue(ctx context.Context, msg []byte, queue string) error {
	return fmt.Errorf("not implemeted")
}

func (broker *Broker) EnqueueScheduled(ctx context.Context, msg []byte, queue string, ts time.Time) error {
	return fmt.Errorf("not implemeted")
}

func (broker *Broker) Consume(ctx context.Context, work chan []byte, queue string) {
}

func (broker *Broker) GetPending(ctx context.Context, queue string) ([]string, error) {
	return nil, fmt.Errorf("not implemeted")
}
