package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"go.ntppool.org/archiver/db"
)

// ArchiveStatus is the data structure from the log_scores_archive_status
// table, keeping track of the last copied log_score for each archive type.
type ArchiveStatus struct {
	ID         int
	Archiver   string
	LogScoreID pgtype.Int8 `db:"log_score_id"`
	ModifiedOn time.Time   `db:"modified_on"`
}

// GetArchiveStatus returns a list of archivers and their status
func GetArchiveStatus(ctx context.Context) ([]ArchiveStatus, error) {
	statuses := []ArchiveStatus{}

	err := db.Pool.Select(ctx, &statuses,
		`select id, archiver, log_score_id, modified_on
		from log_scores_archive_status
		order by log_score_id, modified_on`,
	)
	if err != nil {
		return nil, err
	}

	return statuses, nil
}

// SetStatus updates the "last ID" status for the given archiver
func (status *ArchiveStatus) SetStatus(ctx context.Context, lastID int64) error {
	var logScoreID pgtype.Int8
	if lastID > 0 {
		logScoreID = pgtype.Int8{Int64: lastID, Valid: true}
	}

	tag, err := db.Pool.Exec(ctx,
		`UPDATE log_scores_archive_status
			SET log_score_id = $1, modified_on = NOW() WHERE archiver = $2`,
		logScoreID, status.Archiver,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("archiver %q not found in status table", status.Archiver)
	}
	status.ModifiedOn = time.Now()
	status.LogScoreID = logScoreID
	return nil
}
