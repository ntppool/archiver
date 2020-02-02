package clickhouse

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/kshvakov/clickhouse"

	"go.ntppool.org/archiver/logscore"
	"go.ntppool.org/archiver/storage"
)

// CHArchiver stores log scores in ClickHouse
type CHArchiver struct {
	connect *sql.DB
}

// NewArchiver returns an archiver that stores data in avro files in the specified path
func NewArchiver() (storage.Archiver, error) {
	a := &CHArchiver{}

	dsn := os.Getenv("ch_dsn")
	if len(dsn) == 0 {
		return nil, fmt.Errorf("ch_dsn environment not set")
	}

	connect, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			return nil, fmt.Errorf("[%d] %s \n%s", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}

	_, err = connect.Exec(`
	CREATE TABLE IF NOT EXISTS log_scores (
		dt          Date,
		id 		    UInt64,
		monitor_id  UInt32,
		server_id   UInt32,
		ts	        DateTime,
		score		Float32,
		step 		Float32,
		offset 		Nullable(Float64),
		rtt			Nullable(UInt32),
		leap 		Nullable(UInt8),
		error       Nullable(String)
	) engine=MergeTree
	PARTITION BY dt
	ORDER BY (server_id, ts)
`)
	if err != nil {
		return nil, err
	}

	a.connect = connect

	return a, nil
}

// BatchSizeMinMaxTime returns the minimum and maximum batch size
func (a *CHArchiver) BatchSizeMinMaxTime() (int, int, time.Duration) {
	// inserting data in "real time" is fine according to
	// https://clickhouse.yandex/docs/en/query_language/insert_into/
	// it's just after to do bigger batches, so do up to 500k at once
	return 50, 500000, time.Millisecond * 0
}

// Store sends metrics to ClickHouse
func (a *CHArchiver) Store(logscores []*logscore.LogScore) (int, error) {
	connect := a.connect

	tx, err := connect.Begin()
	if err != nil {
		return 0, err
	}
	stmt, err := tx.Prepare(`
		INSERT INTO log_scores
			(dt, id, server_id, monitor_id, ts, score, step, offset, rtt, leap, error)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	i := 0
	for _, l := range logscores {
		// date := clickhouse.Date(time.Unix(l.Ts, 0).In(time.UTC))

		var leap sql.NullInt64
		if l.Meta.Leap != 0 {
			leap = sql.NullInt64{Int64: l.Meta.Leap, Valid: true}
		}

		var lsError sql.NullString
		if len(l.Meta.Error) > 0 {
			lsError = sql.NullString{String: l.Meta.Error, Valid: true}
		}

		_, err := stmt.Exec(
			l.Ts, // clickhouse figures out the right data in UTC from this
			l.ID, l.ServerID, l.MonitorID,
			l.Ts,
			l.Score, l.Step,
			l.Offset, l.RTT,
			leap, lsError,
		)
		if err != nil {
			return 0, err
		}
		// log.Printf("inserted: %d", result.LastInsertId)
		i++
	}
	err = tx.Commit()
	if err != nil {
		return 0, err
	}

	return i, nil
}

// Close finishes up the archiver
func (a *CHArchiver) Close() error {
	a.connect.Close()
	return nil
}
