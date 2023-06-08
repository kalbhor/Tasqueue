package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/zerodha/logf"
)

const (
	DefaultPollPeriod = time.Second
	sortedSetKey      = "tasqueue:ss:%s"
)

type Options struct {
	Addrs        []string
	Password     string
	DB           int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
	MinIdleConns int
	PollPeriod   time.Duration
}

type Broker struct {
	log        logf.Logger
	conn       redis.UniversalClient
	pollPeriod time.Duration
}

func New(o Options, lo logf.Logger) *Broker {
	pollPeriod := o.PollPeriod
	if o.PollPeriod == 0 {
		pollPeriod = DefaultPollPeriod
	}
	return &Broker{
		log: lo,
		conn: redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:        o.Addrs,
			DB:           o.DB,
			Password:     o.Password,
			DialTimeout:  o.DialTimeout,
			ReadTimeout:  o.ReadTimeout,
			WriteTimeout: o.WriteTimeout,
			MinIdleConns: o.MinIdleConns,
			IdleTimeout:  o.IdleTimeout,
		}),
		pollPeriod: pollPeriod,
	}
}

func (r *Broker) GetPending(ctx context.Context, queue string) ([]string, error) {
	rs, err := r.conn.LRange(ctx, queue, 0, -1).Result()
	if err == redis.Nil {
		return []string{}, nil
	} else if err != nil {
		return []string{}, err
	}

	return rs, nil
}

func (b *Broker) Enqueue(ctx context.Context, msg []byte, queue string) error {
	return b.conn.LPush(ctx, queue, msg).Err()
}

func (b *Broker) EnqueueScheduled(ctx context.Context, msg []byte, queue string, ts time.Time) error {
	return b.conn.ZAdd(ctx, fmt.Sprintf(sortedSetKey, queue), &redis.Z{
		Score:  float64(ts.UnixNano()),
		Member: msg,
	}).Err()
}

func (b *Broker) Consume(ctx context.Context, work chan []byte, queue string) {
	go b.consumeScheduled(ctx, queue)

	for {
		select {
		case <-ctx.Done():
			b.log.Debug("shutting down consumer..")
			return
		default:
			b.log.Debug("receiving from consumer..")
			res, err := b.conn.BLPop(ctx, b.pollPeriod, queue).Result()
			if err != nil && err.Error() != "redis: nil" {
				b.log.Error("error consuming from redis queue", "error", err)
			} else if errors.Is(err, redis.Nil) {
				b.log.Debug("no tasks to consume..", "queue", queue)
			} else {
				msg, err := blpopResult(res)
				if err != nil {
					b.log.Error("error parsing response from redis", "error", err)
					return
				}
				work <- []byte(msg)
			}
		}
	}
}

func (b *Broker) consumeScheduled(ctx context.Context, queue string) {
	poll := time.NewTicker(b.pollPeriod)

	for {
		select {
		case <-ctx.Done():
			b.log.Debug("shutting down scheduled consumer..")
			return
		case <-poll.C:
			b.conn.Watch(ctx, func(tx *redis.Tx) error {
				// Fetch the tasks with score less than current time. These tasks have been scheduled
				// to be queued.
				tasks, err := tx.ZRevRangeByScore(ctx, fmt.Sprintf(sortedSetKey, queue), &redis.ZRangeBy{
					Min:    "0",
					Max:    strconv.FormatInt(time.Now().UnixNano(), 10),
					Offset: 0,
					Count:  1,
				}).Result()
				if err != nil {
					return err
				}

				for _, task := range tasks {
					if err := b.Enqueue(ctx, []byte(task), queue); err != nil {
						return err
					}
				}

				// Remove the tasks
				if err := tx.ZRem(ctx, fmt.Sprintf(sortedSetKey, queue), tasks).Err(); err != nil {
					return err
				}

				return nil
			})
		}

	}
}

func blpopResult(rs []string) (string, error) {
	if len(rs) != 2 {
		return "", fmt.Errorf("BLPop result should have exactly 2 strings. Got : %v", rs)
	}

	return rs[1], nil
}
