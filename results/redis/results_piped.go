package redis

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

const DefaultPipePeriod = 200 * time.Millisecond

type PipedResults struct {
	lo  *slog.Logger
	opt PipedOptions

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
	Expiry       time.Duration
	MetaExpiry   time.Duration
	MinIdleConns int

	PipePeriod time.Duration
}

func NewPiped(o PipedOptions, lo *slog.Logger) *PipedResults {
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

	p := &PipedResults{
		lo:   lo,
		conn: conn,
		pipe: conn.Pipeline(),
		opt:  o,
	}

	// TODO: pass ctx here somehow
	if o.MetaExpiry != 0 {
		go p.expireMeta(context.TODO(), o.MetaExpiry)
	}

	go p.execPipe(context.TODO())

	return p
}

func (r *PipedResults) execPipe(ctx context.Context) {
	tk := time.NewTicker(r.opt.PipePeriod)
	for {
		select {
		case <-ctx.Done():
			r.lo.Info("context closed, draining redis pipe", "length", r.pipe.Len())
			if _, err := r.pipe.Exec(ctx); err != nil {
				r.lo.Error("error executing redis pipe: %v", err)
			}
			return
		case <-tk.C:
			plen := r.pipe.Len()
			if plen == 0 {
				continue
			}

			r.lo.Info("submitting redis pipe", "length", plen)
			if _, err := r.pipe.Exec(ctx); err != nil {
				r.lo.Error("error executing redis pipe: %v", err)
			}
		}
	}
}

func (r *PipedResults) DeleteJob(ctx context.Context, id string) error {
	r.lo.Debug("deleting job")
	if err := r.conn.ZRem(ctx, resultPrefix+success, 1, id).Err(); err != nil {
		return err
	}

	if err := r.conn.ZRem(ctx, resultPrefix+failed, 1, id).Err(); err != nil {
		return err
	}

	if err := r.conn.Del(ctx, resultPrefix+id).Err(); err != nil {
		return err
	}

	return nil
}

func (r *PipedResults) GetSuccess(ctx context.Context) ([]string, error) {
	// Fetch the failed tasks with score less than current time
	r.lo.Debug("getting successful jobs")
	rs, err := r.conn.ZRevRangeByScore(ctx, resultPrefix+success, &redis.ZRangeBy{
		Min: "0",
		Max: strconv.FormatInt(time.Now().UnixNano(), 10),
	}).Result()
	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (r *PipedResults) GetFailed(ctx context.Context) ([]string, error) {
	// Fetch the failed tasks with score less than current time
	r.lo.Debug("getting failed jobs")
	rs, err := r.conn.ZRevRangeByScore(ctx, resultPrefix+failed, &redis.ZRangeBy{
		Min: "0",
		Max: strconv.FormatInt(time.Now().UnixNano(), 10),
	}).Result()
	if err != nil {
		return nil, err
	}

	return rs, nil
}

func (r *PipedResults) SetSuccess(ctx context.Context, id string) error {
	r.lo.Debug("setting job as successful", "id", id)
	return r.pipe.ZAdd(ctx, resultPrefix+success, &redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: id,
	}).Err()
}

func (r *PipedResults) SetFailed(ctx context.Context, id string) error {
	r.lo.Debug("setting job as failed", "id", id)
	return r.pipe.ZAdd(ctx, resultPrefix+failed, &redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: id,
	}).Err()
}

func (r *PipedResults) Set(ctx context.Context, id string, b []byte) error {
	r.lo.Debug("setting result for job", "id", id)
	return r.pipe.Set(ctx, resultPrefix+id, b, r.opt.Expiry).Err()
}

func (r *PipedResults) Get(ctx context.Context, id string) ([]byte, error) {
	r.lo.Debug("getting result for job", "id", id)
	rs, err := r.conn.Get(ctx, resultPrefix+id).Bytes()
	if err != nil {
		return nil, err
	}

	return rs, nil
}

// TODO: accpet a ctx here and shutdown gracefully
func (r *PipedResults) expireMeta(ctx context.Context, ttl time.Duration) {
	r.lo.Info("starting results meta purger", "ttl", ttl)

	var (
		tk = time.NewTicker(ttl)
	)

	for {
		select {
		case <-ctx.Done():
			r.lo.Info("shutting down meta purger", "ttl", ttl)
			return
		case <-tk.C:
			now := time.Now().UnixNano() - int64(ttl)
			score := strconv.FormatInt(now, 10)

			r.lo.Debug("purging failed results metadata", "score", score)
			if err := r.pipe.ZRemRangeByScore(context.Background(), resultPrefix+failed, "0", score).Err(); err != nil {
				r.lo.Error("could not expire success/failed metadata", "err", err)
			}

			r.lo.Debug("purging success results metadata", "score", score)
			if err := r.pipe.ZRemRangeByScore(context.Background(), resultPrefix+success, "0", score).Err(); err != nil {
				r.lo.Error("could not expire success/failed metadata", "err", err)
			}
		}
	}
}

func (r *PipedResults) NilError() error {
	return redis.Nil
}
