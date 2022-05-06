package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"go.zerodha.tech/commons/go-logger"
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
	errLogger  *logger.Logger
	infoLogger *logger.Logger
	conn       redis.UniversalClient
}

func DefaultRedis() Options {
	return Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}
}

func New(o Options, errLogger *logger.Logger, infoLogger *logger.Logger) *Broker {
	if errLogger == nil {
		errLogger = logger.New(logger.Production, "kronika-error")
	}
	if infoLogger == nil {
		infoLogger = logger.New(logger.Info, "kronika-info")
	}
	return &Broker{
		infoLogger: infoLogger,
		errLogger:  errLogger,
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
			b.infoLogger.Info("shutting down consumer..")
			return
		default:
			b.infoLogger.Info("receiving from consumer..")
			res, err := b.conn.BLPop(ctx, pollPeriod, queue).Result()
			if err != nil {
				b.errLogger.ErrGeneral("error consuming from redis queue", err).Write()
			} else if errors.Is(err, redis.Nil) {
				b.infoLogger.Info(queue + ": no tasks to consume..")
			} else {
				msg, err := blpopResult(res)
				if err != nil {
					b.errLogger.ErrGeneral("error parsing response from redis", err).Write()
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
