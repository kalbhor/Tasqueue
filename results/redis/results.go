package redis

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	resultPrefix = "tq:res:"

	// Suffix for hashmaps storing success/failed job ids
	success = "success"
	failed  = "failed"

	// Prefix for task metadata
	taskPrefix = "tq:task:"
	// Key for set of all task names
	tasksSet = "tq:tasks"
)

type Results struct {
	opts Options
	lo   *slog.Logger
	conn redis.UniversalClient
	pipe redis.Pipeliner
}

type Options struct {
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

	// OPTIONAL
	// If non-zero, enqueue redis commands will be piped instead of being directly sent each time.
	// The pipe will be executed every `PipePeriod` duration.
	PipePeriod time.Duration
}

func DefaultRedis() Options {
	return Options{
		Addrs:    []string{"127.0.0.1:6379"},
		Password: "",
		DB:       0,
	}
}

func New(o Options, lo *slog.Logger) *Results {
	rs := &Results{
		opts: o,
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

	// TODO: pass ctx here somehow
	if o.MetaExpiry != 0 {
		go rs.expireMeta(o.MetaExpiry)
	}
	if o.PipePeriod != 0 {
		rs.pipe = rs.conn.Pipeline()
		go rs.execPipe(context.TODO())
	}

	return rs
}

func (r *Results) execPipe(ctx context.Context) {
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

func (r *Results) DeleteJob(ctx context.Context, id string) error {
	r.lo.Debug("deleting job")

	pipe := r.conn.Pipeline()
	if err := pipe.ZRem(ctx, resultPrefix+success, 1, id).Err(); err != nil {
		return err
	}
	if err := pipe.ZRem(ctx, resultPrefix+failed, 1, id).Err(); err != nil {
		return err
	}
	if err := pipe.Del(ctx, resultPrefix+id).Err(); err != nil {
		return err
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	return nil
}

func (r *Results) GetSuccess(ctx context.Context) ([]string, error) {
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

func (r *Results) GetFailed(ctx context.Context) ([]string, error) {
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

func (r *Results) SetSuccess(ctx context.Context, id string) error {
	r.lo.Debug("setting job as successful", "id", id)
	if r.opts.PipePeriod != 0 {
		return r.pipe.ZAdd(ctx, resultPrefix+success, &redis.Z{
			Score:  float64(time.Now().UnixNano()),
			Member: id,
		}).Err()
	}
	return r.conn.ZAdd(ctx, resultPrefix+success, &redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: id,
	}).Err()
}

func (r *Results) SetFailed(ctx context.Context, id string) error {
	r.lo.Debug("setting job as failed", "id", id)
	if r.opts.PipePeriod != 0 {
		return r.pipe.ZAdd(ctx, resultPrefix+failed, &redis.Z{
			Score:  float64(time.Now().UnixNano()),
			Member: id,
		}).Err()
	}
	return r.conn.ZAdd(ctx, resultPrefix+failed, &redis.Z{
		Score:  float64(time.Now().UnixNano()),
		Member: id,
	}).Err()
}

func (r *Results) Set(ctx context.Context, id string, b []byte) error {
	r.lo.Debug("setting result for job", "id", id)
	if r.opts.PipePeriod != 0 {
		return r.pipe.Set(ctx, resultPrefix+id, b, r.opts.Expiry).Err()
	}
	return r.conn.Set(ctx, resultPrefix+id, b, r.opts.Expiry).Err()
}

func (r *Results) Get(ctx context.Context, id string) ([]byte, error) {
	r.lo.Debug("getting result for job", "id", id)
	rs, err := r.conn.Get(ctx, resultPrefix+id).Bytes()
	if err != nil {
		return nil, err
	}

	return rs, nil
}

// TODO: accpet a ctx here and shutdown gracefully
func (r *Results) expireMeta(ttl time.Duration) {
	r.lo.Info("starting results meta purger", "ttl", ttl)

	var (
		tk = time.NewTicker(ttl)
	)

	for {
		select {
		// case <-ctx.Done():
		// 	r.lo.Info("shutting down meta purger", "ttl", ttl)
		// 	return
		case <-tk.C:
			now := time.Now().UnixNano() - int64(ttl)
			score := strconv.FormatInt(now, 10)

			r.lo.Debug("purging failed results metadata", "score", score)
			if r.opts.PipePeriod != 0 {
				if err := r.pipe.ZRemRangeByScore(context.Background(), resultPrefix+failed, "0", score).Err(); err != nil {
					r.lo.Error("could not expire success/failed metadata", "err", err)
				}
				r.lo.Debug("purging success results metadata", "score", score)
				if err := r.pipe.ZRemRangeByScore(context.Background(), resultPrefix+success, "0", score).Err(); err != nil {
					r.lo.Error("could not expire success/failed metadata", "err", err)
				}
			} else {
				if err := r.conn.ZRemRangeByScore(context.Background(), resultPrefix+failed, "0", score).Err(); err != nil {
					r.lo.Error("could not expire success/failed metadata", "err", err)
				}
				r.lo.Debug("purging success results metadata", "score", score)
				if err := r.conn.ZRemRangeByScore(context.Background(), resultPrefix+success, "0", score).Err(); err != nil {
					r.lo.Error("could not expire success/failed metadata", "err", err)
				}
			}
		}
	}
}

func (r *Results) NilError() error {
	return redis.Nil
}

// SetTask stores task metadata in Redis
func (r *Results) SetTask(ctx context.Context, name string, task []byte) error {
	r.lo.Debug("setting task metadata", "name", name)

	pipe := r.conn.Pipeline()
	// Store the task metadata
	if err := pipe.Set(ctx, taskPrefix+name, task, 0).Err(); err != nil {
		return err
	}
	// Add task name to the set of all tasks
	if err := pipe.SAdd(ctx, tasksSet, name).Err(); err != nil {
		return err
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	return nil
}

// GetTask retrieves task metadata from Redis
func (r *Results) GetTask(ctx context.Context, name string) ([]byte, error) {
	r.lo.Debug("getting task metadata", "name", name)
	rs, err := r.conn.Get(ctx, taskPrefix+name).Bytes()
	if err != nil {
		return nil, err
	}

	return rs, nil
}

// GetAllTasks retrieves all task metadata from Redis
func (r *Results) GetAllTasks(ctx context.Context) ([][]byte, error) {
	r.lo.Debug("getting all task metadata")

	// Get all task names from the set
	taskNames, err := r.conn.SMembers(ctx, tasksSet).Result()
	if err != nil {
		return nil, err
	}

	if len(taskNames) == 0 {
		return [][]byte{}, nil
	}

	// Build keys for all tasks
	keys := make([]string, len(taskNames))
	for i, name := range taskNames {
		keys[i] = taskPrefix + name
	}

	// Fetch all task metadata in one go using MGet
	results, err := r.conn.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	// Convert results to [][]byte
	tasks := make([][]byte, 0, len(results))
	for _, result := range results {
		if result == nil {
			continue
		}
		if taskBytes, ok := result.(string); ok {
			tasks = append(tasks, []byte(taskBytes))
		}
	}

	return tasks, nil
}

// DeleteTask removes task metadata from Redis
func (r *Results) DeleteTask(ctx context.Context, name string) error {
	r.lo.Debug("deleting task metadata", "name", name)

	pipe := r.conn.Pipeline()
	// Remove the task metadata
	if err := pipe.Del(ctx, taskPrefix+name).Err(); err != nil {
		return err
	}
	// Remove task name from the set
	if err := pipe.SRem(ctx, tasksSet, name).Err(); err != nil {
		return err
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	return nil
}
