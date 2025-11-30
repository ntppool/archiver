package source

import (
	"context"
	"fmt"
	"time"

	"go.ntppool.org/archiver/db"
	"go.ntppool.org/archiver/storage"
	"go.ntppool.org/common/logger"
)

type Cleaner interface {
	Interval() time.Duration
	Run(context.Context, *Source, storage.ArchiveStatus) error
}

type Cleanup struct {
	RetentionDays int
	interval      time.Duration
}

var (
	defaultInterval  = 4 * time.Minute
	cleanupBatchSize = 200000
)

func (c *Cleanup) Interval() time.Duration {
	if c.interval == 0 {
		return defaultInterval
	}
	return c.interval
}

func (c *Cleanup) Run(ctx context.Context, source *Source, status storage.ArchiveStatus) error {
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

	r, err := db.Pool.Exec(ctx,
		`DELETE FROM log_scores
		WHERE id IN (
			SELECT id FROM log_scores
			WHERE ts < NOW() - INTERVAL '1 day' * $1
			  AND id < (SELECT MIN(log_score_id) FROM log_scores_archive_status)
			ORDER BY id
			LIMIT $2
		)`,
		maxDays, cleanupBatchSize,
	)
	if err != nil {
		return fmt.Errorf("cleanup error: %s", err)
	}

	rowCount := r.RowsAffected()
	log.Info("cleaned rows", "count", rowCount)

	// todo: this doesn't do anything because state isn't
	// kept from run to run, so the interval is always reset
	if rowCount == int64(cleanupBatchSize) {
		c.interval = 1 * time.Minute
	}

	err = status.SetStatus(ctx, 0)
	if err != nil {
		return fmt.Errorf("could not update archiver status for %q : %s", status.Archiver, err)
	}

	return nil
}
