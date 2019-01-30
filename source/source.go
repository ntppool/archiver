package source

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/ntppool/archiver"
	"github.com/ntppool/archiver/db"
	"github.com/ntppool/archiver/logscore"
	"github.com/ntppool/archiver/storage"
)

type Source struct {
	Table string
}

func New(table string) *Source {
	return &Source{Table: table}
}

func (source *Source) Process(s storage.ArchiveStatus) error {
	log.Printf("processing %s", s.Archiver)
	arch, err := archiver.SetupArchiver(s.Archiver, "")
	if err != nil || arch == nil {
		log.Printf("setup '%s' archiver: %s", s.Archiver, err)
		return err
	}
	minSize, maxSize := arch.BatchSizeMinMax()

	lastID := int64(0)

	hasAttributes, err := source.checkAttributes()
	if err != nil {
		return err
	}
	log.Printf("Has attributes: %t", hasAttributes)

	// check that there are min entries to copy
	var count int
	if s.LogScoreID.Valid && s.LogScoreID.Int64 > 0 {
		lastID = s.LogScoreID.Int64
		log.Printf("getting count after %d from %s", s.LogScoreID.Int64, source.Table)
		err := db.DB.Get(&count,
			fmt.Sprintf("select count(*) from %s where id > ?", source.Table),
			s.LogScoreID)
		if err != nil {
			log.Fatalf("db err: %s", err)
		}
	} else {
		log.Printf("getting full count from %s", source.Table)
		err := db.DB.Get(&count,
			fmt.Sprintf("select count(*) from %s", source.Table),
		)
		if err != nil {
			log.Fatalf("db err: %s", err)
		}
	}
	if count < minSize {
		log.Printf("Only %d entries available in %s (%s needs %d)",
			count, source.Table, s.Archiver, minSize,
		)
		return nil
	}

	if count > maxSize {
		log.Printf("has more than max")
	}

	for count > minSize {
		log.Printf("Count: %d, minSize: %d", count, minSize)

		log.Printf("Fetching up to %d LogScores from %s with id > %d",
			maxSize, source.Table, lastID,
		)

		fields := `id,monitor_id,server_id,UNIX_TIMESTAMP(ts),score,step,offset`
		if hasAttributes {
			fields = fields + ",attributes"
		}

		rows, err := db.DB.Query(
			fmt.Sprintf(
				`select %s
				from %s
				where 
				  id > ?
				order by id
				limit ?`,
				fields,
				source.Table,
			),
			lastID,
			maxSize,
		)
		if err != nil {
			log.Fatalf("select error: %s", err)
		}

		logScores := []*logscore.LogScore{}

		for rows.Next() {

			var monitorID sql.NullInt64
			var offset sql.NullFloat64
			var attributes sql.RawBytes

			ls := logscore.LogScore{}

			// todo: add new meta data column

			fields := []interface{}{&ls.ID, &monitorID, &ls.ServerID, &ls.Ts, &ls.Score, &ls.Step, &offset}
			if hasAttributes {
				fields = append(fields, &attributes)
			}

			err := rows.Scan(fields...)
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

			if len(attributes) > 0 {
				err = json.Unmarshal(attributes, &ls.Meta)
				if err != nil {
					log.Fatalf("error unmarshal'ing %q: %s", attributes, err)
				}
			}

			logScores = append(logScores, &ls)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			log.Fatal(err)
		}

		if len(logScores) == 0 {
			// this shouldn't happen, so just in case?
			log.Printf("No log scores to process for %s", s.Archiver)
			return nil
		}

		log.Printf("Storing %d log scores", len(logScores))

		cnt, err := arch.Store(logScores)
		log.Printf("%s saved %d scores", s.Archiver, cnt)
		if err != nil {
			return err
		}

		newLastID := logScores[len(logScores)-1].ID
		log.Printf("Setting new Last ID to %d (was %d)", newLastID, lastID)
		err = storage.SetArchiveStatus(s.Archiver, newLastID)
		if err != nil {
			return fmt.Errorf("Could not update archiver status for %q to %d: %s",
				s.Archiver, newLastID, err)
		}

		// do another batch if there's more data
		lastID = newLastID
		count = count - len(logScores)
	}

	return nil
}

func (source *Source) checkAttributes() (bool, error) {

	type TableStruct struct {
		Field   string         `db:"Field"`
		Type    string         `db:"Type"`
		Null    string         `db:"Null"`
		Key     string         `db:"Key"`
		Default sql.NullString `db:"Default"`
		Extra   string         `db:"Extra"`
	}

	columns := []TableStruct{}

	err := db.DB.Select(&columns, fmt.Sprintf("DESCRIBE %s", source.Table))
	if err != nil {
		return false, fmt.Errorf("describe error: %s", err)
	}

	for _, c := range columns {
		if c.Field == "attributes" {
			return true, nil
		}
	}

	return false, nil
}
