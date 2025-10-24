package redis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
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

	// OPTIONAL
	// If non-zero, enqueue redis commands will be piped instead of being directly sent each time.
	// The pipe will be executed every `PipePeriod` duration.
	PipePeriod time.Duration
}

type Broker struct {
	lo   *slog.Logger
	opts Options

	conn redis.UniversalClient
	pipe redis.Pipeliner
}

func New(o Options, lo *slog.Logger) *Broker {
	if o.PollPeriod == 0 {
		o.PollPeriod = DefaultPollPeriod
	}
	b := &Broker{
		opts: o,
		lo:   lo,
		conn: redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:           o.Addrs,
			DB:              o.DB,
			Password:        o.Password,
			DialTimeout:     o.DialTimeout,
			ReadTimeout:     o.ReadTimeout,
			WriteTimeout:    o.WriteTimeout,
			MinIdleConns:    o.MinIdleConns,
			ConnMaxIdleTime: o.IdleTimeout,
		}),
	}

	if o.PipePeriod != 0 {
		b.pipe = b.conn.Pipeline()
		go b.execPipe(context.TODO())
	}

	return b
}

func (r *Broker) execPipe(ctx context.Context) {
	tk := time.NewTicker(r.opts.PipePeriod)
	for {
		select {
		case <-ctx.Done():
			r.lo.Debug("context closed, draining redis pipe", "length", r.pipe.Len())
			if _, err := r.pipe.Exec(ctx); err != nil {
				r.lo.Error("could not execute redis pipe", "error", err)
			}
			return
		case <-tk.C:
			plen := r.pipe.Len()
			if plen == 0 {
				continue
			}
			r.lo.Debug("submitting redis pipe", "length", plen)
			if _, err := r.pipe.Exec(ctx); err != nil {
				r.lo.Error("could not execute redis pipe", "error", err)
			}
		}
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
	if b.opts.PipePeriod != 0 {
		return b.pipe.LPush(ctx, queue, msg).Err()
	}
	return b.conn.LPush(ctx, queue, msg).Err()
}

func (b *Broker) EnqueueScheduled(ctx context.Context, msg []byte, queue string, ts time.Time) error {
	if b.opts.PipePeriod != 0 {
		return b.pipe.ZAdd(ctx, fmt.Sprintf(sortedSetKey, queue), redis.Z{
			Score:  float64(ts.UnixNano()),
			Member: msg,
		}).Err()
	}
	return b.conn.ZAdd(ctx, fmt.Sprintf(sortedSetKey, queue), redis.Z{
		Score:  float64(ts.UnixNano()),
		Member: msg,
	}).Err()
}

func (b *Broker) Consume(ctx context.Context, work chan []byte, queue string) {
	// Ensure work channel is closed when producer (broker) is done
	defer close(work)
	go b.consumeScheduled(ctx, queue)

	for {
		select {
		case <-ctx.Done():
			b.lo.Debug("shutting down consumer..")
			return
		default:
			b.lo.Debug("receiving from consumer..")
			res, err := b.conn.BLPop(ctx, b.opts.PollPeriod, queue).Result()
			if err != nil && err.Error() != "redis: nil" {
				b.lo.Error("error consuming from redis queue", "error", err)
			} else if errors.Is(err, redis.Nil) {
				b.lo.Debug("no tasks to consume..", "queue", queue)
			} else {
				msg, err := blpopResult(res)
				if err != nil {
					b.lo.Error("error parsing response from redis", "error", err)
					return
				}
				work <- []byte(msg)
			}
		}
	}
}

func (b *Broker) consumeScheduled(ctx context.Context, queue string) {
	poll := time.NewTicker(b.opts.PollPeriod)

	for {
		select {
		case <-ctx.Done():
			b.lo.Debug("shutting down scheduled consumer..")
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
