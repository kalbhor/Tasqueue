package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

type Broker struct {
	db  *sql.DB
	log *slog.Logger
}

type Options struct {
	DataSource string
}

func New(options Options, lo *slog.Logger) (*Broker, error) {
	db, err := sql.Open("sqlite3", options.DataSource)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`
        CREATE TABLE IF NOT EXISTS jobs(
            id VARCHAR PRIMARY KEY,
            queue VARCHAR NOT NULL,
            msg TEXT NOT NULL,
            timestamp DATE DEFAULT(datetime('now', 'localtime'))
        );
    `)
	if err != nil {
		return nil, err
	}

	return &Broker{
		db:  db,
		log: lo,
	}, nil
}

func (broker *Broker) Enqueue(ctx context.Context, msg []byte, queue string) error {
	return fmt.Errorf("not implemeted")
}

func (broker *Broker) EnqueueScheduled(ctx context.Context, msg []byte, queue string, ts time.Time) error {
	return fmt.Errorf("not implemeted")
}

func (broker *Broker) Consume(ctx context.Context, work chan []byte, queue string) {
}

func (broker *Broker) GetPending(ctx context.Context, queue string) ([]string, error) {
	return nil, fmt.Errorf("not implemeted")
}
