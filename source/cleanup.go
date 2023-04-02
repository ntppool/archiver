package source

import (
	"fmt"
	"log"
	"time"

	"go.ntppool.org/archiver/db"
	"go.ntppool.org/archiver/storage"
)

type Cleaner interface {
	Interval() time.Duration
	Run(*Source, storage.ArchiveStatus)
}

type Cleanup struct {
	RetentionDays int
}

func (c *Cleanup) Interval() time.Duration {
	return 4 * time.Minute
}

func (c *Cleanup) Run(source *Source, status storage.ArchiveStatus) error {
	interval := c.Interval()
	if next := tooSoon(status.ModifiedOn, interval); !next.IsZero() {
		log.Printf("Don't run cleaner until %s", next)
		return nil
	}

	log.Printf("running cleaner")

	maxDays := c.RetentionDays
	if maxDays < 3 {
		log.Printf("retention days set too low (%d), resetting to 3")
		maxDays = 3
	}

	r, err := db.DB.Exec(
		`delete
		from log_scores
		where
		  ts < date_sub(now(), interval ? day)
		  and id < (select min(log_score_id) from log_scores_archive_status)
		order by id
		limit 100000`,
		maxDays,
	)
	if err != nil {
		return fmt.Errorf("cleanup error: %s", err)
	}

	rowCount, err := r.RowsAffected()
	log.Printf("cleaned up %d rows", rowCount)
	if err != nil {
		return fmt.Errorf("could not get row count: %s", err)
	}

	err = status.SetStatus(0)
	if err != nil {
		return fmt.Errorf("could not update archiver status for %q : %s", status.Archiver, err)
	}

	return nil
}
