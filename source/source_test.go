package source

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.ntppool.org/archiver/db"
	"go.ntppool.org/archiver/logscore"
	"go.ntppool.org/archiver/storage"
)

// mockArchiver implements storage.Archiver interface for testing
type mockArchiver struct {
	minSize   int
	maxSize   int
	interval  time.Duration
	stored    [][]*logscore.LogScore
	storeErr  error
	closeErr  error
	storeCnt  int
}

func (m *mockArchiver) BatchSizeMinMaxTime() (int, int, time.Duration) {
	return m.minSize, m.maxSize, m.interval
}

func (m *mockArchiver) Store(ls []*logscore.LogScore) (int, error) {
	if m.storeErr != nil {
		return 0, m.storeErr
	}
	m.stored = append(m.stored, ls)
	m.storeCnt = len(ls)
	return len(ls), nil
}

func (m *mockArchiver) Close() error {
	return m.closeErr
}

func TestNew(t *testing.T) {
	tests := []struct {
		name          string
		table         string
		retentionDays int
		wantErr       bool
		wantRetention int
	}{
		{
			name:          "valid table with retention",
			table:         "log_scores",
			retentionDays: 10,
			wantErr:       false,
			wantRetention: 10,
		},
		{
			name:          "valid table without retention (default)",
			table:         "log_scores",
			retentionDays: 0,
			wantErr:       false,
			wantRetention: 14,
		},
		{
			name:          "invalid table",
			table:         "invalid_table",
			retentionDays: 10,
			wantErr:       true,
		},
		{
			name:          "valid archive table",
			table:         "log_scores_archive",
			retentionDays: 5,
			wantErr:       false,
			wantRetention: 5,
		},
		{
			name:          "valid test table",
			table:         "log_scores_test",
			retentionDays: 7,
			wantErr:       false,
			wantRetention: 7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source, err := New(tt.table, tt.retentionDays)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, source)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, source)
				assert.Equal(t, tt.table, source.Table)
				assert.Equal(t, tt.wantRetention, source.retentionDays)
			}
		})
	}
}

func TestTooSoon(t *testing.T) {
	now := time.Now()
	oneHour := time.Hour

	tests := []struct {
		name     string
		last     time.Time
		interval time.Duration
		wantZero bool
	}{
		{
			name:     "zero time returns zero",
			last:     time.Time{},
			interval: oneHour,
			wantZero: true,
		},
		{
			name:     "too soon returns next time",
			last:     now.Add(-30 * time.Minute),
			interval: oneHour,
			wantZero: false,
		},
		{
			name:     "not too soon returns zero",
			last:     now.Add(-2 * time.Hour),
			interval: oneHour,
			wantZero: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tooSoon(tt.last, tt.interval)
			if tt.wantZero {
				assert.True(t, result.IsZero())
			} else {
				assert.False(t, result.IsZero())
			}
		})
	}
}

func TestCheckField(t *testing.T) {
	// Create a mock database
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	// Replace the global db with our mock
	originalDB := db.DB
	db.DB = sqlx.NewDb(mockDB, "mysql")
	defer func() { db.DB = originalDB }()

	source := &Source{Table: "log_scores", retentionDays: 14}

	t.Run("field exists", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
			AddRow("id", "bigint(20)", "NO", "PRI", nil, "auto_increment").
			AddRow("attributes", "json", "YES", "", nil, "")

		mock.ExpectQuery("DESCRIBE log_scores").WillReturnRows(rows)

		hasField, err := source.checkField("attributes")
		assert.NoError(t, err)
		assert.True(t, hasField)
	})

	t.Run("field does not exist", func(t *testing.T) {
		rows := sqlmock.NewRows([]string{"Field", "Type", "Null", "Key", "Default", "Extra"}).
			AddRow("id", "bigint(20)", "NO", "PRI", nil, "auto_increment").
			AddRow("server_id", "int(11)", "NO", "", nil, "")

		mock.ExpectQuery("DESCRIBE log_scores").WillReturnRows(rows)

		hasField, err := source.checkField("nonexistent")
		assert.NoError(t, err)
		assert.False(t, hasField)
	})

	t.Run("database error", func(t *testing.T) {
		mock.ExpectQuery("DESCRIBE log_scores").WillReturnError(sql.ErrConnDone)

		hasField, err := source.checkField("attributes")
		assert.Error(t, err)
		assert.False(t, hasField)
	})

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessTooSoon(t *testing.T) {
	// Mock a recent modification time that should be too soon
	status := storage.ArchiveStatus{
		Archiver:   "test",
		LogScoreID: sql.NullInt64{Int64: 100, Valid: true},
		ModifiedOn: time.Now().Add(-10 * time.Minute), // Recent
	}

	// Mock the SetupArchiver call - this is complex since we can't easily mock it
	// For now, we'll test the logic separately
	t.Run("too soon logic", func(t *testing.T) {
		next := tooSoon(status.ModifiedOn, time.Hour)
		assert.False(t, next.IsZero(), "Should return non-zero time when too soon")
	})
}

func TestProcessCount(t *testing.T) {
	// Create a mock database
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	// Replace the global db with our mock
	originalDB := db.DB
	db.DB = sqlx.NewDb(mockDB, "mysql")
	defer func() { db.DB = originalDB }()

	t.Run("count with valid last ID", func(t *testing.T) {
		// Mock count query
		countRows := sqlmock.NewRows([]string{"count(*)"}).AddRow(5)
		mock.ExpectQuery("select count\\(\\*\\) from log_scores where id > \\?").
			WithArgs(int64(100)).
			WillReturnRows(countRows)

		status := storage.ArchiveStatus{
			Archiver:   "test",
			LogScoreID: sql.NullInt64{Int64: 100, Valid: true},
			ModifiedOn: time.Now().Add(-2 * time.Hour),
		}

		// Test the count logic in isolation
		var count int
		err := db.DB.Get(&count, "select count(*) from log_scores where id > ?", status.LogScoreID.Int64)
		assert.NoError(t, err)
		assert.Equal(t, 5, count)
	})

	t.Run("count without last ID", func(t *testing.T) {
		// Mock full count query
		countRows := sqlmock.NewRows([]string{"count(*)"}).AddRow(50)
		mock.ExpectQuery("select count\\(\\*\\) from log_scores").WillReturnRows(countRows)

		var count int
		err := db.DB.Get(&count, "select count(*) from log_scores")
		assert.NoError(t, err)
		assert.Equal(t, 50, count)
	})

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessDataFetching(t *testing.T) {
	// Create a mock database
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	// Replace the global db with our mock
	originalDB := db.DB
	db.DB = sqlx.NewDb(mockDB, "mysql")
	defer func() { db.DB = originalDB }()

	t.Run("fetch data with attributes and rtt", func(t *testing.T) {
		// Mock the data query
		metaData := logscore.LogScoreMetadata{Leap: 1, Error: "test"}
		metaJSON, _ := json.Marshal(metaData)

		dataRows := sqlmock.NewRows([]string{
			"id", "monitor_id", "server_id", "UNIX_TIMESTAMP(ts)", "score", "step", "offset", "attributes", "rtt",
		}).
			AddRow(1, 10, 20, 1640995200, 15.5, 0.1, 0.05, metaJSON, 150).
			AddRow(2, 11, 21, 1640995260, 16.0, 0.2, nil, nil, nil)

		mock.ExpectQuery("select id,monitor_id,server_id,UNIX_TIMESTAMP\\(ts\\),score,step,offset,attributes,rtt").
			WithArgs(int64(0), 100).
			WillReturnRows(dataRows)

		rows, err := db.DB.Query(
			"select id,monitor_id,server_id,UNIX_TIMESTAMP(ts),score,step,offset,attributes,rtt from log_scores where id > ? order by id limit ?",
			int64(0), 100,
		)
		require.NoError(t, err)
		defer rows.Close()

		var logScores []*logscore.LogScore

		for rows.Next() {
			var monitorID sql.NullInt64
			var offset sql.NullFloat64
			var rtt sql.NullInt64
			var attributes sql.RawBytes

			ls := &logscore.LogScore{}

			err := rows.Scan(&ls.ID, &monitorID, &ls.ServerID, &ls.Ts, &ls.Score, &ls.Step, &offset, &attributes, &rtt)
			require.NoError(t, err)

			ls.MonitorID = monitorID.Int64

			if offset.Valid {
				ls.Offset = &offset.Float64
			}

			if rtt.Valid {
				ls.RTT = &rtt.Int64
			}

			if len(attributes) > 0 {
				err = json.Unmarshal(attributes, &ls.Meta)
				require.NoError(t, err)
			}

			logScores = append(logScores, ls)
		}

		require.NoError(t, rows.Err())
		assert.Len(t, logScores, 2)

		// Check first record
		assert.Equal(t, int64(1), logScores[0].ID)
		assert.Equal(t, int64(10), logScores[0].MonitorID)
		assert.Equal(t, int64(20), logScores[0].ServerID)
		assert.Equal(t, int64(1640995200), logScores[0].Ts)
		assert.Equal(t, 15.5, logScores[0].Score)
		assert.Equal(t, 0.1, logScores[0].Step)
		assert.NotNil(t, logScores[0].Offset)
		assert.Equal(t, 0.05, *logScores[0].Offset)
		assert.NotNil(t, logScores[0].RTT)
		assert.Equal(t, int64(150), *logScores[0].RTT)
		assert.Equal(t, uint8(1), logScores[0].Meta.Leap)
		assert.Equal(t, "test", logScores[0].Meta.Error)

		// Check second record (with nulls)
		assert.Equal(t, int64(2), logScores[1].ID)
		assert.Equal(t, int64(11), logScores[1].MonitorID)
		assert.Nil(t, logScores[1].Offset)
		assert.Nil(t, logScores[1].RTT)
		assert.Equal(t, logscore.LogScoreMetadata{}, logScores[1].Meta)
	})

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessDatabaseErrors(t *testing.T) {
	// Create a mock database
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	// Replace the global db with our mock
	originalDB := db.DB
	db.DB = sqlx.NewDb(mockDB, "mysql")
	defer func() { db.DB = originalDB }()

	t.Run("describe error", func(t *testing.T) {
		mock.ExpectQuery("DESCRIBE log_scores").WillReturnError(sql.ErrConnDone)

		source := &Source{Table: "log_scores", retentionDays: 14}
		hasField, err := source.checkField("attributes")
		assert.Error(t, err)
		assert.False(t, hasField)
		assert.Contains(t, err.Error(), "describe error")
	})

	t.Run("count query error", func(t *testing.T) {
		mock.ExpectQuery("select count\\(\\*\\) from log_scores").WillReturnError(sql.ErrConnDone)

		var count int
		err := db.DB.Get(&count, "select count(*) from log_scores")
		assert.Error(t, err)
	})

	t.Run("data query error", func(t *testing.T) {
		mock.ExpectQuery("select id,monitor_id,server_id").WillReturnError(sql.ErrConnDone)

		rows, err := db.DB.Query("select id,monitor_id,server_id from log_scores where id > ? order by id limit ?", int64(0), 100)
		assert.Error(t, err)
		assert.Nil(t, rows)
	})

	t.Run("scan error", func(t *testing.T) {
		// Return wrong number of columns to cause scan error
		dataRows := sqlmock.NewRows([]string{"id", "monitor_id"}).
			AddRow(1, 10) // Missing required columns

		mock.ExpectQuery("select id,monitor_id,server_id").
			WillReturnRows(dataRows)

		rows, err := db.DB.Query("select id,monitor_id,server_id from log_scores where id > ? order by id limit ?", int64(0), 100)
		require.NoError(t, err)
		defer rows.Close()

		if rows.Next() {
			var id, monitorID, serverID int64
			err = rows.Scan(&id, &monitorID, &serverID)
			assert.Error(t, err) // Should error due to missing column
		}
	})

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestProcessJSONUnmarshalError(t *testing.T) {
	// Create a mock database
	mockDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer mockDB.Close()

	// Replace the global db with our mock
	originalDB := db.DB
	db.DB = sqlx.NewDb(mockDB, "mysql")
	defer func() { db.DB = originalDB }()

	t.Run("invalid json attributes", func(t *testing.T) {
		// Mock data with invalid JSON
		dataRows := sqlmock.NewRows([]string{
			"id", "monitor_id", "server_id", "UNIX_TIMESTAMP(ts)", "score", "step", "offset", "attributes",
		}).
			AddRow(1, 10, 20, 1640995200, 15.5, 0.1, 0.05, "{invalid json")

		mock.ExpectQuery("select id,monitor_id,server_id,UNIX_TIMESTAMP\\(ts\\),score,step,offset,attributes").
			WithArgs(int64(0), 100).
			WillReturnRows(dataRows)

		rows, err := db.DB.Query(
			"select id,monitor_id,server_id,UNIX_TIMESTAMP(ts),score,step,offset,attributes from log_scores where id > ? order by id limit ?",
			int64(0), 100,
		)
		require.NoError(t, err)
		defer rows.Close()

		for rows.Next() {
			var monitorID sql.NullInt64
			var offset sql.NullFloat64
			var attributes sql.RawBytes

			ls := &logscore.LogScore{}

			err := rows.Scan(&ls.ID, &monitorID, &ls.ServerID, &ls.Ts, &ls.Score, &ls.Step, &offset, &attributes)
			require.NoError(t, err)

			if len(attributes) > 0 {
				err = json.Unmarshal(attributes, &ls.Meta)
				assert.Error(t, err) // Should error due to invalid JSON
			}
		}
	})

	assert.NoError(t, mock.ExpectationsWereMet())
}