package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
)

const (
	pollPeriod = time.Second
)

type Options struct {
	Addrs    []string
	Password string
	DB       int
}

type Broker struct {
	log  *logrus.Logger
	conn redis.UniversalClient
}

func New(o Options) *Broker {
	return &Broker{
		log: logrus.New(),
		conn: redis.NewClient(
			&redis.Options{
				Addr:     o.Addrs[0],
				Password: o.Password,
				DB:       o.DB,
			},
		),
	}
}

func (b *Broker) Enqueue(ctx context.Context, msg []byte, queue string) error {
	return b.conn.LPush(ctx, queue, msg).Err()
}

func (b *Broker) Consume(ctx context.Context, work chan []byte, queue string) {
	for {
		select {
		case <-ctx.Done():
			b.log.Info("shutting down consumer..")
			return
		default:
			b.log.Info("receiving from consumer..")
			res, err := b.conn.BLPop(ctx, pollPeriod, queue).Result()
			if err != nil && err.Error() != "redis: nil" {
				b.log.Error("error consuming from redis queue", err)
			} else if errors.Is(err, redis.Nil) {
				b.log.Info(queue + ": no tasks to consume..")
			} else {
				msg, err := blpopResult(res)
				if err != nil {
					b.log.Error("error parsing response from redis", err)
					return
				}
				work <- []byte(msg)
			}
		}
	}
}

func blpopResult(rs []string) (string, error) {
	if len(rs) != 2 {
		return "", fmt.Errorf("BLPop result should have exactly 2 strings. Got : %v", rs)
	}

	return rs[1], nil
}
