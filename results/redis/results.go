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

func (r *Results) Get(ctx context.Context, uuid string) ([]byte, error) {
	r.lo.Debug("getting result for job", "uuid", uuid)
	rs, err := r.conn.Get(ctx, resultPrefix+uuid).Result()
	if err != nil {
		return nil, err
	}

	return []byte(rs), nil
}

func (r *Results) Set(ctx context.Context, uuid string, b []byte) error {
	r.lo.Debug("setting result for job", "uuid", uuid)
	return r.conn.Set(ctx, resultPrefix+uuid, b, defaultExpiry).Err()
}
