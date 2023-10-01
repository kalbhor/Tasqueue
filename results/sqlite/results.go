package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	_ "github.com/mattn/go-sqlite3"
)

type ResultStatus string

const (
	QUEUED     ResultStatus = "QUEUED"
	PROCESSING ResultStatus = "PROCESSING"
	FAILED     ResultStatus = "FAILED"
	COMPLETED  ResultStatus = "COMPLETED"
)

type Results struct {
	db  *sql.DB
	log *slog.Logger
}

type Options struct {
	DataSource string
}

func New(options Options, lo *slog.Logger) (*Results, error) {
	db, err := sql.Open("sqlite3", options.DataSource)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS results(
            id VARCHAR PRIMARY KEY,
            msg TEXT NOT NULL
        );
    `)

	return &Results{
		db:  db,
		log: lo,
	}, nil
}

func (results *Results) Get(ctx context.Context, id string) ([]byte, error) {
	return nil, fmt.Errorf("Get: not implemented")
}

func (results *Results) NilError() error {
	return fmt.Errorf("NilError: not implemented")
}

func (results *Results) Set(ctx context.Context, id string, b []byte) error {
	return fmt.Errorf("Set: not implemented")
}

func (results *Results) DeleteJob(ctx context.Context, id string) error {
	return fmt.Errorf("DeleteJob: not implemented")
}

func (results *Results) GetFailed(ctx context.Context) ([]string, error) {
	return nil, fmt.Errorf("GetFailed: not implemented")
}

func (results *Results) GetSuccess(ctx context.Context) ([]string, error) {
	return nil, fmt.Errorf("GetSuccess: not implemented")
}

func (results *Results) SetFailed(ctx context.Context, id string) error {
	return fmt.Errorf("SetFailed: not implemented")
}

func (results *Results) SetSuccess(ctx context.Context, id string) error {
	return fmt.Errorf("SetSuccess: not implemented")
}
