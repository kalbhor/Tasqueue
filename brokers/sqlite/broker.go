package sqlite

import (
	"context"
	"database/sql"
	"errors"
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

func (broker *Broker) Consume(ctx context.Context, work chan []byte, queue string) {
	for {
		select {
		case <-ctx.Done():
			broker.log.Debug("stopping the consumer")
			return
		default:
			msg, err := broker.popJob(ctx, queue)
			if err == nil {
				work <- []byte(msg)
			} else {
				if err == NoJobToProcess {
					broker.log.Debug("no jobs to process")
				} else {
					broker.log.Debug("failed to pop job", msg, err)
					return
				}
			}
		}
	}
}

var NoJobToProcess = errors.New("no jobs to process")

func (broker *Broker) popJob(ctx context.Context, queue string) (string, error) {
	var msg string
	tx, err := broker.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return msg, err
	}
	var id string
	if err = tx.QueryRowContext(ctx, "SELECT id, msg FROM jobs WHERE queue = ? AND status = ?;", queue, tasqueue.StatusStarted).Scan(&id, &msg); err != nil {
		tx.Rollback()
		if err == sql.ErrNoRows {
			return msg, NoJobToProcess
		}
		return msg, err
	}

	result, err := tx.ExecContext(ctx, `UPDATE jobs SET status = ? WHERE id = ?;`, tasqueue.StatusProcessing, id)
	if err != nil {
		tx.Rollback()
		return msg, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		tx.Rollback()
		return msg, err
	}
	if affected == 0 {
		tx.Rollback()
		return msg, NoJobToProcess
	}

	if err = tx.Commit(); err != nil {
		return msg, err
	}

	return msg, nil
}

func (broker *Broker) GetPending(ctx context.Context, queue string) ([]string, error) {
	messages := make([]string, 0)
	rows, err := broker.db.QueryContext(ctx, `SELECT msg FROM jobs WHERE queue = ? AND status = ?`, queue, tasqueue.StatusStarted)
	if err != nil {
		return messages, err
	}
	for rows.Next() {
		var msg string
		if err = rows.Scan(&msg); err != nil {
			return messages, err
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

func (broker *Broker) EnqueueScheduled(ctx context.Context, msg []byte, queue string, ts time.Time) error {
	return fmt.Errorf("EnqueueScheduled: not implemeted")
}
