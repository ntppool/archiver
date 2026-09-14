package clickhouse

import (
	"database/sql"
	"math"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/shopspring/decimal"
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
				uint32(20),       // server_id
				uint32(10),       // monitor_id
				sqlmock.AnyArg(), // ts (time)
				float32(15.5),    // score
				float32(0.1),     // step
				nil,              // offset (null)
				nil,              // rtt (null)
				nil,              // leap (null)
				nil,              // error (null)
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
				uint32(21),                        // server_id
				uint32(11),                        // monitor_id
				sqlmock.AnyArg(),                  // ts (time)
				float32(16.0),                     // score
				float32(0.2),                      // step
				decimal.RequireFromString("0.05"), // offset
				sqlmock.AnyArg(),                  // rtt (uint32 pointer)
				sqlmock.AnyArg(),                  // leap (uint8 pointer)
				"test error",                      // error
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
				uint32(22),       // server_id
				uint32(12),       // monitor_id
				sqlmock.AnyArg(), // ts
				float32(17.0),    // score
				float32(0.3),     // step
				nil,              // offset
				nil,              // rtt
				nil,              // leap
				nil,              // error
			).
			WillReturnResult(sqlmock.NewResult(1, 1))

		// Mock second exec
		mock.ExpectExec("INSERT INTO log_scores").
			WithArgs(
				uint32(23),       // server_id
				uint32(13),       // monitor_id
				sqlmock.AnyArg(), // ts
				float32(18.0),    // score
				float32(0.4),     // step
				nil,              // offset
				nil,              // rtt
				nil,              // leap
				nil,              // error
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
				sqlmock.AnyArg()).
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
				sqlmock.AnyArg()).
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
		})
	}
}

// TestStoreOffset checks the offset argument Store passes for the
// Nullable(Decimal64(7)) column. clickhouse_integration_test.go checks what a
// real server stores.
func TestStoreOffset(t *testing.T) {
	f64 := func(v float64) *float64 { return &v }

	tests := []struct {
		name   string
		offset *float64
		want   any // driver value of the offset argument; nil means NULL
	}{
		{name: "nil", offset: nil, want: nil},
		{name: "rounded to 7 decimals", offset: f64(-0.000256838), want: "-0.0002568"},
		{name: "positive tie rounds away from zero", offset: f64(0.00000015), want: "0.0000002"},
		{name: "negative tie rounds away from zero", offset: f64(-0.00000015), want: "-0.0000002"},
		{name: "inside Decimal64(7) range", offset: f64(-9.99e10), want: "-99900000000"},
		{name: "Decimal64(7) limit", offset: f64(1e11), want: nil},
		{name: "NaN", offset: f64(math.NaN()), want: nil},
		{name: "+Inf", offset: f64(math.Inf(1)), want: nil},
		{name: "-Inf", offset: f64(math.Inf(-1)), want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()

			archiver := &CHArchiver{connect: db}

			mock.ExpectBegin()
			mock.ExpectPrepare("INSERT INTO log_scores")
			mock.ExpectExec("INSERT INTO log_scores").
				WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(),
					sqlmock.AnyArg(), tt.want, sqlmock.AnyArg(), sqlmock.AnyArg(),
					sqlmock.AnyArg()).
				WillReturnResult(sqlmock.NewResult(1, 1))
			mock.ExpectCommit()

			count, err := archiver.Store([]*logscore.LogScore{
				{ID: 1, ServerID: 20, MonitorID: 10, Ts: 1640995200, Offset: tt.offset},
			})
			require.NoError(t, err)
			assert.Equal(t, 1, count)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
