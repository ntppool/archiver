package clickhouse

import (
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.ntppool.org/archiver/logscore"
)

func TestBatchSizeMinMaxTime(t *testing.T) {
	archiver := &CHArchiver{}
	minSize, maxSize, interval := archiver.BatchSizeMinMaxTime()
	assert.Equal(t, 50, minSize)
	assert.Equal(t, 500000, maxSize)
	assert.Equal(t, time.Millisecond*0, interval)
}

func TestNewArchiverMissingDSN(t *testing.T) {
	// Test with missing ch_dsn environment variable
	// Save and restore original env
	originalDSN := os.Getenv("ch_dsn")
	os.Setenv("ch_dsn", "")
	defer os.Setenv("ch_dsn", originalDSN)

	archiver, err := NewArchiver()
	assert.Error(t, err)
	assert.Nil(t, archiver)
	assert.Contains(t, err.Error(), "ch_dsn environment not set")
}

func TestStore(t *testing.T) {
	// Create a mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	archiver := &CHArchiver{connect: db}

	t.Run("empty logscores", func(t *testing.T) {
		// ClickHouse Store still begins transaction even for empty slice
		mock.ExpectBegin()
		mock.ExpectPrepare("INSERT INTO log_scores")
		mock.ExpectCommit()

		count, err := archiver.Store([]*logscore.LogScore{})
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("single logscore", func(t *testing.T) {
		// Mock transaction and prepare
		mock.ExpectBegin()
		mock.ExpectPrepare("INSERT INTO log_scores")

		// Mock the exec call
		mock.ExpectExec("INSERT INTO log_scores").
			WithArgs(
				sqlmock.AnyArg(), // dt (time)
				uint64(123),      // id
				uint32(20),       // server_id
				uint32(10),       // monitor_id
				sqlmock.AnyArg(), // ts (time)
				float32(15.5),    // score
				float32(0.1),     // step
				nil,              // offset (null)
				nil,              // rtt (null)
				nil,              // leap (null)
				sql.NullString{String: "", Valid: false}, // error (null)
			).
			WillReturnResult(sqlmock.NewResult(1, 1))

		mock.ExpectCommit()

		logscores := []*logscore.LogScore{
			{
				ID:        123,
				ServerID:  20,
				MonitorID: 10,
				Ts:        1640995200,
				Score:     15.5,
				Step:      0.1,
				Offset:    nil,
				RTT:       nil,
				Meta:      logscore.LogScoreMetadata{},
			},
		}

		count, err := archiver.Store(logscores)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("logscore with all fields", func(t *testing.T) {
		// Mock transaction and prepare
		mock.ExpectBegin()
		mock.ExpectPrepare("INSERT INTO log_scores")

		offset := 0.05
		rtt := int64(150)
		
		// Mock the exec call with all fields populated
		mock.ExpectExec("INSERT INTO log_scores").
			WithArgs(
				sqlmock.AnyArg(),                         // dt (time)
				uint64(124),                              // id
				uint32(21),                               // server_id
				uint32(11),                               // monitor_id
				sqlmock.AnyArg(),                         // ts (time)
				float32(16.0),                            // score
				float32(0.2),                             // step
				&offset,                                  // offset
				sqlmock.AnyArg(),                         // rtt (uint32 pointer)
				sqlmock.AnyArg(),                         // leap (uint8 pointer)
				sql.NullString{String: "test error", Valid: true}, // error
			).
			WillReturnResult(sqlmock.NewResult(1, 1))

		mock.ExpectCommit()

		logscores := []*logscore.LogScore{
			{
				ID:        124,
				ServerID:  21,
				MonitorID: 11,
				Ts:        1640995260,
				Score:     16.0,
				Step:      0.2,
				Offset:    &offset,
				RTT:       &rtt,
				Meta: logscore.LogScoreMetadata{
					Leap:  1,
					Error: "test error",
				},
			},
		}

		count, err := archiver.Store(logscores)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("multiple logscores", func(t *testing.T) {
		// Mock transaction and prepare
		mock.ExpectBegin()
		mock.ExpectPrepare("INSERT INTO log_scores")

		// Mock first exec
		mock.ExpectExec("INSERT INTO log_scores").
			WithArgs(
				sqlmock.AnyArg(), // dt
				uint64(125),      // id
				uint32(22),       // server_id
				uint32(12),       // monitor_id
				sqlmock.AnyArg(), // ts
				float32(17.0),    // score
				float32(0.3),     // step
				nil,              // offset
				nil,              // rtt
				nil,              // leap
				sql.NullString{String: "", Valid: false}, // error
			).
			WillReturnResult(sqlmock.NewResult(1, 1))

		// Mock second exec
		mock.ExpectExec("INSERT INTO log_scores").
			WithArgs(
				sqlmock.AnyArg(), // dt
				uint64(126),      // id
				uint32(23),       // server_id
				uint32(13),       // monitor_id
				sqlmock.AnyArg(), // ts
				float32(18.0),    // score
				float32(0.4),     // step
				nil,              // offset
				nil,              // rtt
				nil,              // leap
				sql.NullString{String: "", Valid: false}, // error
			).
			WillReturnResult(sqlmock.NewResult(1, 1))

		mock.ExpectCommit()

		logscores := []*logscore.LogScore{
			{
				ID:        125,
				ServerID:  22,
				MonitorID: 12,
				Ts:        1640995320,
				Score:     17.0,
				Step:      0.3,
				Offset:    nil,
				RTT:       nil,
				Meta:      logscore.LogScoreMetadata{},
			},
			{
				ID:        126,
				ServerID:  23,
				MonitorID: 13,
				Ts:        1640995380,
				Score:     18.0,
				Step:      0.4,
				Offset:    nil,
				RTT:       nil,
				Meta:      logscore.LogScoreMetadata{},
			},
		}

		count, err := archiver.Store(logscores)
		assert.NoError(t, err)
		assert.Equal(t, 2, count)
	})

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStoreErrors(t *testing.T) {
	// Create a mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	archiver := &CHArchiver{connect: db}

	t.Run("begin transaction error", func(t *testing.T) {
		mock.ExpectBegin().WillReturnError(sql.ErrConnDone)

		logscores := []*logscore.LogScore{
			{
				ID:        123,
				ServerID:  20,
				MonitorID: 10,
				Ts:        1640995200,
				Score:     15.5,
				Step:      0.1,
			},
		}

		count, err := archiver.Store(logscores)
		assert.Error(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, sql.ErrConnDone, err)
	})

	t.Run("prepare statement error", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectPrepare("INSERT INTO log_scores").WillReturnError(sql.ErrConnDone)
		mock.ExpectRollback()

		logscores := []*logscore.LogScore{
			{
				ID:        123,
				ServerID:  20,
				MonitorID: 10,
				Ts:        1640995200,
				Score:     15.5,
				Step:      0.1,
			},
		}

		count, err := archiver.Store(logscores)
		assert.Error(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, sql.ErrConnDone, err)
	})

	t.Run("exec error", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectPrepare("INSERT INTO log_scores")

		mock.ExpectExec("INSERT INTO log_scores").
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
				sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
				sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnError(sql.ErrConnDone)

		mock.ExpectRollback()

		logscores := []*logscore.LogScore{
			{
				ID:        123,
				ServerID:  20,
				MonitorID: 10,
				Ts:        1640995200,
				Score:     15.5,
				Step:      0.1,
			},
		}

		count, err := archiver.Store(logscores)
		assert.Error(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, sql.ErrConnDone, err)
	})

	t.Run("commit error", func(t *testing.T) {
		mock.ExpectBegin()
		mock.ExpectPrepare("INSERT INTO log_scores")

		mock.ExpectExec("INSERT INTO log_scores").
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
				sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
				sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))

		mock.ExpectCommit().WillReturnError(sql.ErrTxDone)

		logscores := []*logscore.LogScore{
			{
				ID:        123,
				ServerID:  20,
				MonitorID: 10,
				Ts:        1640995200,
				Score:     15.5,
				Step:      0.1,
			},
		}

		count, err := archiver.Store(logscores)
		assert.Error(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, sql.ErrTxDone, err)
	})

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestClose(t *testing.T) {
	// Create a mock database
	db, mock, err := sqlmock.New()
	require.NoError(t, err)

	archiver := &CHArchiver{connect: db}

	// Mock the Close call
	mock.ExpectClose()

	err = archiver.Close()
	assert.NoError(t, err)

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestDataTypeConversions(t *testing.T) {
	tests := []struct {
		name      string
		logscore  *logscore.LogScore
		expectRTT *uint32
		expectErr string
	}{
		{
			name: "nil RTT",
			logscore: &logscore.LogScore{
				ID:        123,
				ServerID:  20,
				MonitorID: 10,
				Ts:        1640995200,
				Score:     15.5,
				Step:      0.1,
				RTT:       nil,
			},
			expectRTT: nil,
			expectErr: "",
		},
		{
			name: "valid RTT",
			logscore: &logscore.LogScore{
				ID:        123,
				ServerID:  20,
				MonitorID: 10,
				Ts:        1640995200,
				Score:     15.5,
				Step:      0.1,
				RTT:       func() *int64 { v := int64(150); return &v }(),
			},
			expectRTT: func() *uint32 { v := uint32(150); return &v }(),
			expectErr: "",
		},
		{
			name: "empty error string",
			logscore: &logscore.LogScore{
				ID:        123,
				ServerID:  20,
				MonitorID: 10,
				Ts:        1640995200,
				Score:     15.5,
				Step:      0.1,
				Meta:      logscore.LogScoreMetadata{Error: ""},
			},
			expectErr: "",
		},
		{
			name: "non-empty error string",
			logscore: &logscore.LogScore{
				ID:        123,
				ServerID:  20,
				MonitorID: 10,
				Ts:        1640995200,
				Score:     15.5,
				Step:      0.1,
				Meta:      logscore.LogScoreMetadata{Error: "test error"},
			},
			expectErr: "test error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test RTT conversion
			var rtt *uint32
			if tt.logscore.RTT != nil {
				urtt := uint32(*tt.logscore.RTT)
				rtt = &urtt
			}

			if tt.expectRTT == nil {
				assert.Nil(t, rtt)
			} else {
				require.NotNil(t, rtt)
				assert.Equal(t, *tt.expectRTT, *rtt)
			}

			// Test error conversion
			var lsError sql.NullString
			if len(tt.logscore.Meta.Error) > 0 {
				lsError = sql.NullString{String: tt.logscore.Meta.Error, Valid: true}
			}

			if tt.expectErr == "" {
				assert.False(t, lsError.Valid)
			} else {
				assert.True(t, lsError.Valid)
				assert.Equal(t, tt.expectErr, lsError.String)
			}

			// Test leap conversion
			var leap *uint8
			if tt.logscore.Meta.Leap != 0 {
				leap = &tt.logscore.Meta.Leap
			}

			if tt.logscore.Meta.Leap == 0 {
				assert.Nil(t, leap)
			} else {
				require.NotNil(t, leap)
				assert.Equal(t, tt.logscore.Meta.Leap, *leap)
			}

			// Test time conversion
			ts := time.Unix(tt.logscore.Ts, 0)
			assert.Equal(t, tt.logscore.Ts, ts.Unix())

			// Test ID conversion
			id := uint64(tt.logscore.ID)
			assert.Equal(t, tt.logscore.ID, int64(id))
		})
	}
}