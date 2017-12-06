package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/ntppool/archiver"
	"github.com/ntppool/archiver/db"
)

func main() {

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", os.Getenv("db_user"), os.Getenv("db_pass"),
		os.Getenv("db_host"), os.Getenv("db_database"),
	)

	err := db.Setup(dsn)
	if err != nil {
		log.Fatalf("database connection: %s", err)
	}

	lock := getLock()
	if !lock {
		log.Printf("Did not get lock, exiting")
		os.Exit(2)
	}

	status, err := archiver.GetArchiveStatus()
	if err != nil {
		log.Fatalf("archive status: %s", err)
	}

	for _, s := range status {
		log.Printf("processing %s", s.Archiver)
		arch, err := archiver.SetupArchiver(s.Archiver, "")
		if err != nil || arch == nil {
			log.Printf("setup '%s' archiver: %s", s.Archiver, err)
			continue
		}
		minSize, maxSize := arch.BatchSizeMinMax()

		// check that there are min entries to copy
		var count int
		if s.LogScoreID.Valid && s.LogScoreID.Int64 > 0 {
			log.Printf("getting count after %d", s.LogScoreID.Int64)
			err := db.DB.Get(&count, "select count(*) from log_scores where id > ?", s.LogScoreID)
			if err != nil {
				log.Fatalf("db err: %s", err)
			}
		} else {
			// log.Println("getting full count")
			// err := db.DB.Get(&count, "select count(*) from log_scores")
			// if err != nil {
			// 	log.Fatalf("db err: %s", err)
			// }
		}
		if count < minSize {
			log.Printf("Only %d entries available (%s needs %d)", count, s.Archiver, minSize)
			continue
		}

		if count > maxSize {
			log.Printf("has more than max")
		}

		// todo: where id > lastID ...
		rows, err := db.DB.Query(
			fmt.Sprintf(`select id,monitor_id,server_id,UNIX_TIMESTAMP(ts),score,step,offset,attributes
				from log_scores
				where ts > 0
				order by id
				limit ?`),
			maxSize,
		)

		logScores := []*archiver.LogScore{}

		for rows.Next() {

			var monitorID sql.NullInt64
			var offset sql.NullFloat64

			ls := archiver.LogScore{}

			// todo: add new meta data column

			err := rows.Scan(&ls.ID, &monitorID, &ls.ServerID, &ls.Ts, &ls.Score, &ls.Step, &offset)
			if err != nil {
				log.Fatal(err)
			}

			// NULL as "0" here is what we want
			ls.MonitorID = monitorID.Int64

			if offset.Valid {
				ls.Offset = &offset.Float64
			} else {
				ls.Offset = nil
			}

			logScores = append(logScores, &ls)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			log.Fatal(err)
		}

		cnt, err := arch.Store(logScores)
		log.Printf("%s saved %d scores", s.Archiver, cnt)
		if err != nil {
			log.Printf("err: %s", err)
			continue
		}
		// update status pointer
	}

}

func getLock() bool {
	// todo: replace with etcd leader
	var lock int
	err := db.DB.Get(&lock, `SELECT GET_LOCK("archiver", 0)`)
	if err != nil {
		log.Fatalf("lock: %s", err)
	}
	if lock == 1 {
		return true
	}
	return false
}
