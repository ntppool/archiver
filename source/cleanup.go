package source

import (
	"fmt"
	"time"

	"go.ntppool.org/archiver/db"
	"go.ntppool.org/archiver/storage"
	"go.ntppool.org/common/logger"
)

type Cleaner interface {
	Interval() time.Duration
	Run(*Source, storage.ArchiveStatus)
}

type Cleanup struct {
	RetentionDays int
	interval      time.Duration
}

var (
	defaultInterval  = 5 * time.Minute
	cleanupBatchSize = 100000
)

func (c *Cleanup) Interval() time.Duration {
	if c.interval == 0 {
		return defaultInterval
	}
	return c.interval
}

func (c *Cleanup) Run(source *Source, status storage.ArchiveStatus) error {
	log := logger.Setup()
	interval := c.Interval()
	if next := tooSoon(status.ModifiedOn, interval); !next.IsZero() {
		log.Debug("Don't run cleaner yet", "next", next)
		return nil
	}

	log.Info("running cleaner")

	maxDays := c.RetentionDays
	if maxDays < 1 {
		log.Warn("retention days set too low, resetting to 1", "setting", maxDays)
		maxDays = 1
	}

	r, err := db.DB.Exec(
		`delete
		from log_scores
		where
		  ts < date_sub(now(), interval ? day)
		  and id < (select min(log_score_id) from log_scores_archive_status)
		order by id
		limit ?`,
		maxDays, cleanupBatchSize,
	)
	if err != nil {
		return fmt.Errorf("cleanup error: %s", err)
	}

	rowCount, err := r.RowsAffected()
	log.Info("cleaned rows", "count", rowCount)
	if err != nil {
		return fmt.Errorf("could not get row count: %s", err)
	}

	// todo: this doesn't do anything because state isn't
	// kept from run to run, so the interval is always reset
	if rowCount == int64(cleanupBatchSize) {
		c.interval = 1 * time.Minute
	}

	err = status.SetStatus(0)
	if err != nil {
		return fmt.Errorf("could not update archiver status for %q : %s", status.Archiver, err)
	}

	return nil
}
