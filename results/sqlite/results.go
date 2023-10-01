package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	_ "github.com/mattn/go-sqlite3"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/kalbhor/tasqueue/v2"
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
            msg TEXT NOT NULL,
            status VARCHAR NOT NULL
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
	var msg tasqueue.JobMessage
	if err := msgpack.Unmarshal(b, &msg); err != nil {
		return err
	}
	tx, err := results.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	result, err := tx.Exec(`REPLACE INTO results(id,msg,status) VALUES(?,?,?);`, id, b, msg.Status)
	if err != nil {
		tx.Rollback()
		return nil
	}
	affected, err := result.RowsAffected()
	if err != nil {
		tx.Rollback()
		return nil
	}
	if affected == 0 {
		tx.Rollback()
		return fmt.Errorf("failed to replace into table %s, %s", id, b)
	}
	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (results *Results) DeleteJob(ctx context.Context, id string) error {
	return fmt.Errorf("DeleteJob: not implemented")
}

func (results *Results) GetFailed(ctx context.Context) ([]string, error) {
	ids := make([]string, 0)
	rows, err := results.db.QueryContext(ctx, `SELECT id FROM results WHERE status = ?;`, tasqueue.StatusFailed)
	if err != nil {
		return ids, err
	}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (results *Results) GetSuccess(ctx context.Context) ([]string, error) {
	return nil, fmt.Errorf("GetSuccess: not implemented")
}

func (results *Results) SetFailed(ctx context.Context, id string) error {
	tx, err := results.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE results SET status = ? WHERE id = ?;`, id, tasqueue.StatusFailed)
	if err != nil {
		tx.Rollback()
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (results *Results) SetSuccess(ctx context.Context, id string) error {
	return fmt.Errorf("SetSuccess: not implemented")
}
