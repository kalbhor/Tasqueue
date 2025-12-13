package nats

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
)

const (
	resultPrefix = "tasqueue-results-"
	taskPrefix   = "tasqueue-task-"
	tasksKey     = "tasqueue-tasks-list"
	kvBucket     = "tasqueue"
)

type Results struct {
	opt  Options
	lo   *slog.Logger
	conn nats.KeyValue
}

type Options struct {
	URL         string
	EnabledAuth bool
	Username    string
	Password    string
}

// New() returns a new instance of nats-jetstream broker.
func New(cfg Options, lo *slog.Logger) (*Results, error) {
	opt := []nats.Option{}

	if cfg.EnabledAuth {
		opt = append(opt, nats.UserInfo(cfg.Username, cfg.Password))
	}

	conn, err := nats.Connect(cfg.URL, opt...)
	if err != nil {
		return nil, fmt.Errorf("error connecting to nats : %w", err)
	}

	// Get jet stream context
	js, err := conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("error creating jetstream context : %w", err)
	}

	kv, err := js.KeyValue(kvBucket)
	if err != nil {
		return nil, fmt.Errorf("error creating key/value bucket : %w", err)
	}

	return &Results{
		opt:  cfg,
		lo:   lo,
		conn: kv,
	}, nil
}

func (r *Results) Get(_ context.Context, id string) ([]byte, error) {
	rs, err := r.conn.Get(resultPrefix + id)
	if err != nil {
		return nil, err
	}

	return rs.Value(), nil
}

func (r *Results) NilError() error {
	return nats.ErrKeyNotFound
}

func (r *Results) Set(_ context.Context, id string, b []byte) error {
	if _, err := r.conn.Put(resultPrefix+id, b); err != nil {
		return err
	}
	return nil
}
func (r *Results) SetSuccess(_ context.Context, id string) error {
	return fmt.Errorf("method not implemented")
}

func (r *Results) SetFailed(_ context.Context, id string) error {
	return fmt.Errorf("method not implemented")
}

func (r *Results) GetSuccess(_ context.Context) ([]string, error) {
	return nil, fmt.Errorf("method not implemented")
}

func (r *Results) GetFailed(_ context.Context) ([]string, error) {
	return nil, fmt.Errorf("method not implemented")
}

func (r *Results) DeleteJob(_ context.Context, id string) error {
	return r.conn.Delete(resultPrefix + id)
}

// SetTask stores task metadata in NATS KV
func (r *Results) SetTask(_ context.Context, name string, task []byte) error {
	// Store the task metadata
	if _, err := r.conn.Put(taskPrefix+name, task); err != nil {
		return err
	}

	// Get existing tasks list
	entry, err := r.conn.Get(tasksKey)
	var tasksList []string
	if err != nil && err != nats.ErrKeyNotFound {
		return err
	}
	if err == nil {
		// Parse existing tasks list
		tasksList = []string{}
		for _, line := range string(entry.Value()) {
			if line != 0 {
				tasksList = append(tasksList, string(line))
			}
		}
	}

	// Add task name if not already in list
	found := false
	for _, t := range tasksList {
		if t == name {
			found = true
			break
		}
	}
	if !found {
		tasksList = append(tasksList, name)
		// Store updated list (simple newline-separated format)
		listBytes := []byte{}
		for i, t := range tasksList {
			if i > 0 {
				listBytes = append(listBytes, '\n')
			}
			listBytes = append(listBytes, []byte(t)...)
		}
		if _, err := r.conn.Put(tasksKey, listBytes); err != nil {
			return err
		}
	}

	return nil
}

// GetTask retrieves task metadata from NATS KV
func (r *Results) GetTask(_ context.Context, name string) ([]byte, error) {
	entry, err := r.conn.Get(taskPrefix + name)
	if err != nil {
		return nil, err
	}

	return entry.Value(), nil
}

// GetAllTasks retrieves all task metadata from NATS KV
func (r *Results) GetAllTasks(_ context.Context) ([][]byte, error) {
	// Get list of task names
	entry, err := r.conn.Get(tasksKey)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return [][]byte{}, nil
		}
		return nil, err
	}

	// Parse task names
	var taskNames []string
	current := []byte{}
	for _, b := range entry.Value() {
		if b == '\n' {
			if len(current) > 0 {
				taskNames = append(taskNames, string(current))
				current = []byte{}
			}
		} else {
			current = append(current, b)
		}
	}
	if len(current) > 0 {
		taskNames = append(taskNames, string(current))
	}

	if len(taskNames) == 0 {
		return [][]byte{}, nil
	}

	// Fetch all task metadata
	tasks := make([][]byte, 0, len(taskNames))
	for _, name := range taskNames {
		entry, err := r.conn.Get(taskPrefix + name)
		if err != nil {
			if err == nats.ErrKeyNotFound {
				continue
			}
			return nil, err
		}
		tasks = append(tasks, entry.Value())
	}

	return tasks, nil
}

// DeleteTask removes task metadata from NATS KV
func (r *Results) DeleteTask(_ context.Context, name string) error {
	// Delete the task metadata
	if err := r.conn.Delete(taskPrefix + name); err != nil {
		return err
	}

	// Get existing tasks list
	entry, err := r.conn.Get(tasksKey)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return nil
		}
		return err
	}

	// Parse and filter out the deleted task
	var taskNames []string
	current := []byte{}
	for _, b := range entry.Value() {
		if b == '\n' {
			if len(current) > 0 {
				taskNames = append(taskNames, string(current))
				current = []byte{}
			}
		} else {
			current = append(current, b)
		}
	}
	if len(current) > 0 {
		taskNames = append(taskNames, string(current))
	}

	// Filter out the task
	filtered := []string{}
	for _, t := range taskNames {
		if t != name {
			filtered = append(filtered, t)
		}
	}

	// Store updated list
	listBytes := []byte{}
	for i, t := range filtered {
		if i > 0 {
			listBytes = append(listBytes, '\n')
		}
		listBytes = append(listBytes, []byte(t)...)
	}
	if _, err := r.conn.Put(tasksKey, listBytes); err != nil {
		return err
	}

	return nil
}
