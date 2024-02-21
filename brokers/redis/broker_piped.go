package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	DefaultPipePeriod = 200 * time.Millisecond
)

type PipeBroker struct {
	log  *slog.Logger
	opts PipedOptions

	conn redis.UniversalClient
	pipe redis.Pipeliner
}

type PipedOptions struct {
	Addrs        []string
	Password     string
	DB           int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
	MinIdleConns int

	PollPeriod time.Duration
	PipePeriod time.Duration
}

func NewPiped(o PipedOptions, lo *slog.Logger) *PipeBroker {
	if o.PollPeriod == 0 {
		o.PollPeriod = DefaultPollPeriod
	}
	if o.PipePeriod == 0 {
		o.PipePeriod = DefaultPipePeriod
	}

	conn := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:        o.Addrs,
		DB:           o.DB,
		Password:     o.Password,
		DialTimeout:  o.DialTimeout,
		ReadTimeout:  o.ReadTimeout,
		WriteTimeout: o.WriteTimeout,
		MinIdleConns: o.MinIdleConns,
		IdleTimeout:  o.IdleTimeout,
	})

	p := &PipeBroker{
		log:  lo,
		conn: conn,
		pipe: conn.Pipeline(),
		opts: o,
	}

	go p.pushPipe(context.TODO())

	return p
}

func (r *PipeBroker) pushPipe(ctx context.Context) {
	tk := time.NewTicker(r.opts.PipePeriod)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			r.log.Debug("submitting redis pipe")
			if r.pipe.Len() == 0 {
				continue
			}
			if _, err := r.pipe.Exec(ctx); err != nil {
				r.log.Error("error executing redis pipe: %v", err)
			}
		}
	}
}

func (r *PipeBroker) GetPending(ctx context.Context, queue string) ([]string, error) {
	rs, err := r.conn.LRange(ctx, queue, 0, -1).Result()
	if err == redis.Nil {
		return []string{}, nil
	} else if err != nil {
		return []string{}, err
	}

	return rs, nil
}

func (b *PipeBroker) Enqueue(ctx context.Context, msg []byte, queue string) error {
	return b.pipe.LPush(ctx, queue, msg).Err()
}

func (b *PipeBroker) EnqueueScheduled(ctx context.Context, msg []byte, queue string, ts time.Time) error {
	return b.pipe.ZAdd(ctx, fmt.Sprintf(sortedSetKey, queue), &redis.Z{
		Score:  float64(ts.UnixNano()),
		Member: msg,
	}).Err()
}

func (b *PipeBroker) Consume(ctx context.Context, work chan []byte, queue string) {
	go b.consumeScheduled(ctx, queue)

	for {
		select {
		case <-ctx.Done():
			b.log.Debug("shutting down consumer..")
			return
		default:
			b.log.Debug("receiving from consumer..")
			res, err := b.conn.BLPop(ctx, b.opts.PollPeriod, queue).Result()
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

func (b *PipeBroker) consumeScheduled(ctx context.Context, queue string) {
	poll := time.NewTicker(b.opts.PollPeriod)

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
