package storage

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.ntppool.org/archiver/db"
)

// TestDatabaseConnectionFailures tests various database connection failure scenarios
func TestDatabaseConnectionFailures(t *testing.T) {
	t.Run("GetArchiveStatus database error", func(t *testing.T) {
		// Create a mock database
		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()

		// Replace the global db with our mock
		originalDB := db.DB
		db.DB = sqlx.NewDb(mockDB, "mysql")
		defer func() { db.DB = originalDB }()

		// Mock database error
		mock.ExpectQuery("select id, archiver, log_score_id, modified_on").
			WillReturnError(sql.ErrConnDone)

		statuses, err := GetArchiveStatus()
		assert.Error(t, err)
		assert.Nil(t, statuses)
		assert.Equal(t, sql.ErrConnDone, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("GetArchiveStatus successful", func(t *testing.T) {
		// Create a mock database
		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()

		// Replace the global db with our mock
		originalDB := db.DB
		db.DB = sqlx.NewDb(mockDB, "mysql")
		defer func() { db.DB = originalDB }()

		// Mock successful response
		rows := sqlmock.NewRows([]string{"id", "archiver", "log_score_id", "modified_on"}).
			AddRow(1, "fileavro", 12345, time.Now()).
			AddRow(2, "clickhouse", nil, time.Now())

		mock.ExpectQuery("select id, archiver, log_score_id, modified_on").
			WillReturnRows(rows)

		statuses, err := GetArchiveStatus()
		assert.NoError(t, err)
		assert.Len(t, statuses, 2)
		assert.Equal(t, "fileavro", statuses[0].Archiver)
		assert.Equal(t, "clickhouse", statuses[1].Archiver)
		assert.True(t, statuses[0].LogScoreID.Valid)
		assert.False(t, statuses[1].LogScoreID.Valid)

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("SetStatus database error", func(t *testing.T) {
		// Create a mock database
		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()

		// Replace the global db with our mock
		originalDB := db.DB
		db.DB = sqlx.NewDb(mockDB, "mysql")
		defer func() { db.DB = originalDB }()

		status := &ArchiveStatus{
			ID:         1,
			Archiver:   "test",
			LogScoreID: sql.NullInt64{Int64: 100, Valid: true},
			ModifiedOn: time.Now(),
		}

		// Mock database error
		mock.ExpectExec("update log_scores_archive_status").
			WithArgs(sql.NullInt64{Int64: 200, Valid: true}, "test").
			WillReturnError(sql.ErrConnDone)

		err = status.SetStatus(200)
		assert.Error(t, err)
		assert.Equal(t, sql.ErrConnDone, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("SetStatus successful", func(t *testing.T) {
		// Create a mock database
		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()

		// Replace the global db with our mock
		originalDB := db.DB
		db.DB = sqlx.NewDb(mockDB, "mysql")
		defer func() { db.DB = originalDB }()

		status := &ArchiveStatus{
			ID:         1,
			Archiver:   "test",
			LogScoreID: sql.NullInt64{Int64: 100, Valid: true},
			ModifiedOn: time.Now(),
		}

		// Mock successful update
		mock.ExpectExec("update log_scores_archive_status").
			WithArgs(sql.NullInt64{Int64: 200, Valid: true}, "test").
			WillReturnResult(sqlmock.NewResult(1, 1))

		err = status.SetStatus(200)
		assert.NoError(t, err)
		assert.Equal(t, int64(200), status.LogScoreID.Int64)
		assert.True(t, status.LogScoreID.Valid)

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("SetStatus with zero lastID", func(t *testing.T) {
		// Create a mock database
		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()

		// Replace the global db with our mock
		originalDB := db.DB
		db.DB = sqlx.NewDb(mockDB, "mysql")
		defer func() { db.DB = originalDB }()

		status := &ArchiveStatus{
			ID:         1,
			Archiver:   "test",
			LogScoreID: sql.NullInt64{Int64: 100, Valid: true},
			ModifiedOn: time.Now(),
		}

		// Mock successful update with null value
		mock.ExpectExec("update log_scores_archive_status").
			WithArgs(sql.NullInt64{Valid: false}, "test").
			WillReturnResult(sqlmock.NewResult(1, 1))

		err = status.SetStatus(0)
		assert.NoError(t, err)
		assert.False(t, status.LogScoreID.Valid)

		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestLockAcquisitionFailures tests MySQL GET_LOCK failure scenarios
func TestLockAcquisitionFailures(t *testing.T) {
	t.Run("GET_LOCK database error", func(t *testing.T) {
		// Create a mock database
		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()

		// Replace the global db with our mock
		originalDB := db.DB
		db.DB = sqlx.NewDb(mockDB, "mysql")
		defer func() { db.DB = originalDB }()

		// Mock database error for GET_LOCK
		mock.ExpectQuery("SELECT GET_LOCK\\(\\?, 0\\)").
			WithArgs("test-lock").
			WillReturnError(sql.ErrConnDone)

		// Test the lock acquisition
		var lock int
		err = db.DB.Get(&lock, `SELECT GET_LOCK(?, 0)`, "test-lock")
		assert.Error(t, err)
		assert.Equal(t, sql.ErrConnDone, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("GET_LOCK returns 0 (lock not acquired)", func(t *testing.T) {
		// Create a mock database
		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()

		// Replace the global db with our mock
		originalDB := db.DB
		db.DB = sqlx.NewDb(mockDB, "mysql")
		defer func() { db.DB = originalDB }()

		// Mock lock not acquired (returns 0)
		rows := sqlmock.NewRows([]string{"GET_LOCK(?, 0)"}).AddRow(0)
		mock.ExpectQuery("SELECT GET_LOCK\\(\\?, 0\\)").
			WithArgs("test-lock").
			WillReturnRows(rows)

		// Test the lock acquisition
		var lock int
		err = db.DB.Get(&lock, `SELECT GET_LOCK(?, 0)`, "test-lock")
		assert.NoError(t, err)
		assert.Equal(t, 0, lock)

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("GET_LOCK returns 1 (lock acquired)", func(t *testing.T) {
		// Create a mock database
		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()

		// Replace the global db with our mock
		originalDB := db.DB
		db.DB = sqlx.NewDb(mockDB, "mysql")
		defer func() { db.DB = originalDB }()

		// Mock lock acquired (returns 1)
		rows := sqlmock.NewRows([]string{"GET_LOCK(?, 0)"}).AddRow(1)
		mock.ExpectQuery("SELECT GET_LOCK\\(\\?, 0\\)").
			WithArgs("test-lock").
			WillReturnRows(rows)

		// Test the lock acquisition
		var lock int
		err = db.DB.Get(&lock, `SELECT GET_LOCK(?, 0)`, "test-lock")
		assert.NoError(t, err)
		assert.Equal(t, 1, lock)

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("GET_LOCK returns NULL (error)", func(t *testing.T) {
		// Create a mock database
		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()

		// Replace the global db with our mock
		originalDB := db.DB
		db.DB = sqlx.NewDb(mockDB, "mysql")
		defer func() { db.DB = originalDB }()

		// Mock lock error (returns NULL)
		rows := sqlmock.NewRows([]string{"GET_LOCK(?, 0)"}).AddRow(nil)
		mock.ExpectQuery("SELECT GET_LOCK\\(\\?, 0\\)").
			WithArgs("test-lock").
			WillReturnRows(rows)

		// Test the lock acquisition
		var lock sql.NullInt64
		err = db.DB.Get(&lock, `SELECT GET_LOCK(?, 0)`, "test-lock")
		assert.NoError(t, err)
		assert.False(t, lock.Valid) // NULL value

		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestNetworkFailures tests network-related failure scenarios
func TestNetworkFailures(t *testing.T) {
	t.Run("connection timeout", func(t *testing.T) {
		// Create a mock database
		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()

		// Replace the global db with our mock
		originalDB := db.DB
		db.DB = sqlx.NewDb(mockDB, "mysql")
		defer func() { db.DB = originalDB }()

		// Mock connection timeout
		mock.ExpectQuery("select count\\(\\*\\) from log_scores").
			WillReturnError(sql.ErrConnDone)

		var count int
		err = db.DB.Get(&count, "select count(*) from log_scores")
		assert.Error(t, err)
		assert.Equal(t, sql.ErrConnDone, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("transaction rollback on connection failure", func(t *testing.T) {
		// Create a mock database
		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()

		// Replace the global db with our mock
		originalDB := db.DB
		db.DB = sqlx.NewDb(mockDB, "mysql")
		defer func() { db.DB = originalDB }()

		// Mock transaction begin, then connection failure
		mock.ExpectBegin()
		mock.ExpectPrepare("INSERT INTO log_scores").WillReturnError(sql.ErrConnDone)
		mock.ExpectRollback()

		// Simulate transaction usage
		tx, err := db.DB.Begin()
		assert.NoError(t, err)

		_, err = tx.Prepare("INSERT INTO log_scores VALUES (?)")
		assert.Error(t, err)
		assert.Equal(t, sql.ErrConnDone, err)

		err = tx.Rollback()
		assert.NoError(t, err)

		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestDataIntegrityFailures tests data integrity and validation failure scenarios
func TestDataIntegrityFailures(t *testing.T) {
	t.Run("malformed JSON in attributes field", func(t *testing.T) {
		// Create a mock database
		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()

		// Replace the global db with our mock
		originalDB := db.DB
		db.DB = sqlx.NewDb(mockDB, "mysql")
		defer func() { db.DB = originalDB }()

		// Mock data with invalid JSON
		rows := sqlmock.NewRows([]string{
			"id", "monitor_id", "server_id", "UNIX_TIMESTAMP(ts)", "score", "step", "offset", "attributes",
		}).
			AddRow(1, 10, 20, 1640995200, 15.5, 0.1, 0.05, "{invalid json")

		mock.ExpectQuery("select id,monitor_id,server_id,UNIX_TIMESTAMP\\(ts\\),score,step,offset,attributes").
			WithArgs(int64(0), 100).
			WillReturnRows(rows)

		rows_result, err := db.DB.Query(
			"select id,monitor_id,server_id,UNIX_TIMESTAMP(ts),score,step,offset,attributes from log_scores where id > ? order by id limit ?",
			int64(0), 100,
		)
		require.NoError(t, err)
		defer rows_result.Close()

		// Test that JSON unmarshaling fails gracefully
		for rows_result.Next() {
			var attributes sql.RawBytes
			var id, monitorID, serverID, ts int64
			var score, step, offset float64

			err := rows_result.Scan(&id, &monitorID, &serverID, &ts, &score, &step, &offset, &attributes)
			require.NoError(t, err)

			if len(attributes) > 0 {
				var meta map[string]interface{}
				err = json.Unmarshal(attributes, &meta)
				assert.Error(t, err) // Should fail due to invalid JSON
			}
		}

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("database constraint violation", func(t *testing.T) {
		// Create a mock database
		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()

		// Replace the global db with our mock
		originalDB := db.DB
		db.DB = sqlx.NewDb(mockDB, "mysql")
		defer func() { db.DB = originalDB }()

		// Mock constraint violation error
		mock.ExpectExec("update log_scores_archive_status").
			WithArgs(sql.NullInt64{Int64: 200, Valid: true}, "test").
			WillReturnError(&mysql.MySQLError{
				Number:  1062,
				Message: "Duplicate entry",
			})

		status := &ArchiveStatus{
			ID:         1,
			Archiver:   "test",
			LogScoreID: sql.NullInt64{Int64: 100, Valid: true},
			ModifiedOn: time.Now(),
		}

		err = status.SetStatus(200)
		assert.Error(t, err)
		// In a real scenario, we'd check for specific MySQL error types

		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestRecoveryScenarios tests how the system handles and recovers from various error conditions
func TestRecoveryScenarios(t *testing.T) {
	t.Run("retry after temporary database failure", func(t *testing.T) {
		// Create a mock database
		mockDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer mockDB.Close()

		// Replace the global db with our mock
		originalDB := db.DB
		db.DB = sqlx.NewDb(mockDB, "mysql")
		defer func() { db.DB = originalDB }()

		// First call fails, second succeeds
		mock.ExpectQuery("select count\\(\\*\\) from log_scores").
			WillReturnError(sql.ErrConnDone)

		// Second attempt succeeds
		rows := sqlmock.NewRows([]string{"count(*)"}).AddRow(100)
		mock.ExpectQuery("select count\\(\\*\\) from log_scores").
			WillReturnRows(rows)

		// First attempt fails
		var count int
		err = db.DB.Get(&count, "select count(*) from log_scores")
		assert.Error(t, err)

		// Second attempt succeeds
		err = db.DB.Get(&count, "select count(*) from log_scores")
		assert.NoError(t, err)
		assert.Equal(t, 100, count)

		assert.NoError(t, mock.ExpectationsWereMet())
	})
}
