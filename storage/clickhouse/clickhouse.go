package clickhouse

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/kshvakov/clickhouse"

	"github.com/ntppool/archiver/logscore"
	"github.com/ntppool/archiver/storage"
)

// CHArchiver stores log scores in ClickHouse
type CHArchiver struct {
}

// NewArchiver returns an archiver that stores data in avro files in the specified path
func NewArchiver() (storage.Archiver, error) {
	a := &CHArchiver{}
	return a, nil
}

// BatchSizeMinMax returns the minimum and maximum batch size for InfluxArchiver
func (a *CHArchiver) BatchSizeMinMax() (int, int) {
	return 5000, 100000
}

// Store sends metrics to ClickHouse
func (a *CHArchiver) Store(logscores []*logscore.LogScore) (int, error) {

	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?debug=true")
	if err != nil {
		log.Fatal(err)
	}
	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			return 0, fmt.Errorf("[%d] %s \n%s", exception.Code, exception.Message, exception.StackTrace)
		}
		return 0, err
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
		leap 		Nullable(UInt8)
	) engine=MergeTree
	PARTITION BY dt
	ORDER BY (server_id, ts)

`)
	if err != nil {
		log.Fatal(err)
	}
	tx, err := connect.Begin()
	if err != nil {
		return 0, err
	}
	stmt, err := tx.Prepare(`
		INSERT INTO log_scores
			(dt, id, server_id, monitor_id, ts, score, step, offset, leap)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	i := 0
	for _, l := range logscores {
		date := clickhouse.Date(time.Unix(l.Ts, 0))
		var leap sql.NullInt64
		if l.Meta.Leap != 0 {
			leap = sql.NullInt64{Int64: l.Meta.Leap, Valid: true}
		}

		_, err := stmt.Exec(date,
			l.ID, l.ServerID, l.MonitorID,
			l.Ts,
			l.Score, l.Step, l.Offset,
			leap,
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
