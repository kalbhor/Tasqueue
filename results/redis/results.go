package redis

import (
	"context"

	"github.com/go-redis/redis/v8"
)

const (
	defaultExpiry = 0
	resultPrefix  = "tasqueue:results:"
)

type Results struct {
	conn redis.UniversalClient
}

type Options struct {
	Addrs    []string
	Password string
	DB       int
}

func DefaultRedis() Options {
	return Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}
}

func New(o Options) *Results {
	return &Results{
		conn: redis.NewClient(
			&redis.Options{
				Addr:     o.Addrs[0],
				Password: o.Password,
				DB:       o.DB,
			},
		),
	}
}

func (r *Results) Get(ctx context.Context, uuid string) ([]byte, error) {
	rs, err := r.conn.Get(ctx, resultPrefix+uuid).Result()
	if err != nil {
		return nil, err
	}

	return []byte(rs), nil
}

func (r *Results) Set(ctx context.Context, uuid string, b []byte) error {
	return r.conn.Set(ctx, resultPrefix+uuid, b, defaultExpiry).Err()
}
