package nats

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/zerodha/logf"
)

const (
	resultPrefix = "tasqueue-results-"
	kvBucket     = "tasqueue"
)

type Results struct {
	opt  Options
	lo   logf.Logger
	conn nats.KeyValue
}

type Options struct {
	URL         string
	EnabledAuth bool
	Username    string
	Password    string
}

// New() returns a new instance of nats-jetstream broker.
func New(cfg Options, lo logf.Logger) (*Results, error) {
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

	kv, err := js.KeyValue(kvBucket)
	if err != nil {
		return nil, fmt.Errorf("error creating key/value bucket : %w", err)
	}

	return &Results{
		opt:  cfg,
		lo:   lo,
		conn: kv,
	}, nil
}

func (r *Results) Get(_ context.Context, id string) ([]byte, error) {
	rs, err := r.conn.Get(resultPrefix + id)
	if err != nil {
		return nil, err
	}

	return rs.Value(), nil
}

func (r *Results) NilError() error {
	return nats.ErrKeyNotFound
}

func (r *Results) Set(_ context.Context, id string, b []byte) error {
	if _, err := r.conn.Put(resultPrefix+id, b); err != nil {
		return err
	}
	return nil
}
func (r *Results) SetSuccess(_ context.Context, id string) error {
	return fmt.Errorf("method not implemented")
}

func (r *Results) SetFailed(_ context.Context, id string) error {
	return fmt.Errorf("method not implemented")
}

func (r *Results) GetSuccess(_ context.Context) ([]string, error) {
	return nil, fmt.Errorf("method not implemented")
}

func (r *Results) GetFailed(_ context.Context) ([]string, error) {
	return nil, fmt.Errorf("method not implemented")
}

func (r *Results) DeleteJob(_ context.Context, id string) error {
	return r.conn.Delete(resultPrefix + id)
}
