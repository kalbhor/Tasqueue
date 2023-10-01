package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/kalbhor/tasqueue/v2"
)

type JobStatus string

const (
	QUEUED     JobStatus = "QUEUED"
	PROCESSING JobStatus = "PROCESSING"
	FAILED     JobStatus = "FAILED"
	COMPLETED  JobStatus = "COMPLETED"
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
            status VARCHAT NOT NULL,
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
	var job_msg tasqueue.JobMessage
	if err := msgpack.Unmarshal(msg, &job_msg); err != nil {
		return err
	}
	tx, err := broker.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		tx.Rollback()
		broker.log.Debug("failed to begin the transaction", err)
		return err
	}
	result, err := tx.Exec(`REPLACE INTO jobs(id,queue,msg,status) VALUES(?,?,?,?);`, job_msg.ID, queue, msg, job_msg.Status)
	if err != nil {
		tx.Rollback()
		broker.log.Debug("failed to replace in jobs", err)
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		tx.Rollback()
		broker.log.Debug("failed get affected rows jobs", err)
		return err
	}
	if affected == 0 {
		tx.Rollback()
		broker.log.Debug("no rows affected")
		return fmt.Errorf("failed to replace the job %s, %s", job_msg.ID, msg)
	}
	if err = tx.Commit(); err != nil {
		broker.log.Debug("failed commit the transaction", err)
		return err
	}
	return nil
}

func (broker *Broker) EnqueueScheduled(ctx context.Context, msg []byte, queue string, ts time.Time) error {
	return fmt.Errorf("EnqueueScheduled: not implemeted")
}

func (broker *Broker) Consume(ctx context.Context, work chan []byte, queue string) {
}

func (broker *Broker) GetPending(ctx context.Context, queue string) ([]string, error) {
	return nil, fmt.Errorf("GetPending: not implemeted")
}
