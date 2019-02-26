package storage

import (
	"database/sql"
	"time"

	"github.com/kr/pretty"
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

	pretty.Println(statuses)

	return statuses, nil
}

// SetArchiveStatus updates the "last ID" status for the given archiver
func SetArchiveStatus(archiver string, lastID int64) error {
	_, err := db.DB.Exec(
		`update log_scores_archive_status
		set log_score_id=? where archiver=?`,
		lastID, archiver,
	)
	// log.Printf("%+v", r)
	return err
}
