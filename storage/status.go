package storage

import (
	"database/sql"
	"time"

	"go.ntppool.org/archiver/db"
)

// ArchiveStatus is the data structure from the log_scores_archive_status
// table, keeping track of the last copied log_score for each archive type.
type ArchiveStatus struct {
	ID         int
	Archiver   string
	LogScoreID sql.NullInt64 `db:"log_score_id"`
	ModifiedOn time.Time     `db:"modified_on"`
}

// GetArchiveStatus returns a list of archivers and their status
func GetArchiveStatus() ([]ArchiveStatus, error) {
	statuses := []ArchiveStatus{}

	err := db.DB.Select(&statuses,
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
func (status *ArchiveStatus) SetStatus(lastID int64) error {
	var logScoreID sql.NullInt64
	if lastID > 0 {
		logScoreID = sql.NullInt64{Int64: lastID, Valid: true}
	}

	_, err := db.DB.Exec(
		`update log_scores_archive_status
			set log_score_id=?, modified_on=NOW() where archiver=?`,
		logScoreID, status.Archiver,
	)
	if err != nil {
		return err
	}
	status.ModifiedOn = time.Now()
	status.LogScoreID = logScoreID
	return nil
}
