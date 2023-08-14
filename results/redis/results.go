package redis

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/zerodha/logf"
)

const (
	defaultExpiry = 0
	resultPrefix  = "tasqueue:results:"

	// Suffix for hashmaps storing success/failed job ids
	success = "success"
	failed  = "failed"
)

type Results struct {
	opt  Options
	lo   logf.Logger
	conn redis.UniversalClient
}

type Options struct {
	Addrs        []string
	Password     string
	DB           int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
	MinIdleConns int
}

func DefaultRedis() Options {
	return Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}
}

func New(o Options, lo logf.Logger) *Results {
	return &Results{
		opt: o,
		conn: redis.NewUniversalClient(
			&redis.UniversalOptions{
				Addrs:        o.Addrs,
				Password:     o.Password,
				DB:           o.DB,
				DialTimeout:  o.DialTimeout,
				ReadTimeout:  o.ReadTimeout,
				WriteTimeout: o.WriteTimeout,
				IdleTimeout:  o.IdleTimeout,
				MinIdleConns: o.MinIdleConns,
			},
		),
		lo: lo,
	}
}

func (r *Results) GetSuccess(ctx context.Context) ([]string, error) {
	r.lo.Debug("getting successful jobs")
	rs, err := r.conn.LRange(ctx, resultPrefix+success, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (r *Results) DeleteJob(ctx context.Context, id string) error {
	r.lo.Debug("deleting job")
	if err := r.conn.LRem(ctx, resultPrefix+success, 1, id).Err(); err != nil {
		return err
	}

	if err := r.conn.LRem(ctx, resultPrefix+failed, 1, id).Err(); err != nil {
		return err
	}

	if err := r.conn.Del(ctx, resultPrefix+id).Err(); err != nil {
		return err
	}

	return nil
}

func (r *Results) GetFailed(ctx context.Context) ([]string, error) {
	r.lo.Debug("getting failed jobs")
	rs, err := r.conn.LRange(ctx, resultPrefix+failed, 0, -1).Result()
	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (r *Results) SetSuccess(ctx context.Context, id string) error {
	r.lo.Debug("setting job as successful")
	_, err := r.conn.RPush(ctx, resultPrefix+success, id).Result()
	if err != nil {
		return err
	}

	return nil
}

func (r *Results) SetFailed(ctx context.Context, id string) error {
	r.lo.Debug("setting job as failed")
	_, err := r.conn.RPush(ctx, resultPrefix+failed, id).Result()
	if err != nil {
		return err
	}

	return nil
}

func (r *Results) Set(ctx context.Context, id string, b []byte) error {
	r.lo.Debug("setting result for job", "id", id)
	return r.conn.Set(ctx, resultPrefix+id, b, defaultExpiry).Err()
}

func (r *Results) Get(ctx context.Context, id string) ([]byte, error) {
	r.lo.Debug("getting result for job", "id", id)

	rs, err := r.conn.Get(ctx, resultPrefix+id).Bytes()
	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (r *Results) NilError() error {
	return redis.Nil
}
