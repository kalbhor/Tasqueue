package nats

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/zerodha/logf"
)

// Broker is a nats-jetstream based broker implementation.
type Broker struct {
	opt  Options
	log  logf.Logger
	conn nats.JetStreamContext
}

type Options struct {
	URL         string
	EnabledAuth bool
	Username    string
	Password    string

	// Stream -> Subjects map
	Streams map[string][]string
}

// New() returns a new instance of nats-jetstream broker.
func New(cfg Options, lo logf.Logger) (*Broker, error) {
	opt := []nats.Option{}

	if cfg.EnabledAuth {
		opt = append(opt, nats.UserInfo(cfg.Username, cfg.Password))
	}

	conn, err := nats.Connect(cfg.URL, opt...)
	if err != nil {
		return nil, fmt.Errorf("error connecting to nats : %w", err)
	}

	// Get jet stream context
	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("error creating jetstream context : %w", err)
	}

	// Create streams and add subjects to stream for persistence.
	for k, v := range cfg.Streams {
		if _, err := js.AddStream(&nats.StreamConfig{
			Name:     k,
			Subjects: v,
		}); err != nil {
			return nil, err
		}
	}

	return &Broker{
		opt:  cfg,
		conn: js,
		log:  lo,
	}, nil
}

// // UpdateStream() adds additional subjects to a stream if stream exists.
// // Otherwise it creates a new stream and adds the subjects.
// func (b *Broker) UpdateStream(stream string, subjects []string) error {
// 	b.opt.mu.Lock()
// 	subs, ok := b.opt.Streams[stream]
// 	b.opt.mu.Unlock()

// 	if ok {
// 		subs = append(subs, subjects...)
// 		if _, err := b.conn.UpdateStream(&nats.StreamConfig{
// 			Name:     stream,
// 			Subjects: subs,
// 		}); err != nil {
// 			return err
// 		}
// 	} else {
// 		if _, err := b.conn.AddStream(&nats.StreamConfig{
// 			Name:     stream,
// 			Subjects: subjects,
// 		}); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

func (b *Broker) Enqueue(_ context.Context, msg []byte, queue string) error {
	if _, err := b.conn.Publish(queue, msg); err != nil {
		return err
	}
	return nil
}

func (b *Broker) Consume(ctx context.Context, work chan []byte, queue string) {
	_, err := b.conn.Subscribe(queue, func(msg *nats.Msg) {
		work <- msg.Data
	}, nats.Durable(queue), nats.AckExplicit())
	if err != nil {
		b.log.Error("error consuming from nats", "error", err)
	}

	<-ctx.Done()
	b.log.Debug("shutting down consumer..")
}
