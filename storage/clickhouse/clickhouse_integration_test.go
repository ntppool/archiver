//go:build integration

package clickhouse

import (
	"database/sql"
	"math"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.ntppool.org/archiver/logscore"
)

// TestStoreIntegration inserts through the real driver into the schema in
// schema/<database>/. The sqlmock tests never reach clickhouse-go's column
// encoders, so they can't tell whether a Go value is accepted by
// Nullable(Decimal64(7)), LowCardinality(Nullable(String)) and so on.
//
// Run it with scripts/test-clickhouse-integration.sh.
func TestStoreIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_CLICKHOUSE_DSN")
	if dsn == "" {
		t.Fatal("TEST_CLICKHOUSE_DSN not set; run scripts/test-clickhouse-integration.sh")
	}
	requireLoopback(t, dsn)
	t.Setenv("ch_dsn", dsn)

	archiver, err := NewArchiver()
	require.NoError(t, err)
	defer archiver.Close()

	db, err := sql.Open("clickhouse", dsn)
	require.NoError(t, err)
	defer db.Close()

	str := func(s string) *string { return &s }
	f64 := func(v float64) *float64 { return &v }
	i64 := func(v int64) *int64 { return &v }

	tests := []struct {
		name       string
		offset     *float64
		rtt        *int64
		leap       uint8
		lsError    string
		wantOffset *string // nil means NULL
	}{
		{name: "nullable fields empty"},
		{name: "offset rounded to 7 decimals", offset: f64(-0.000256838), wantOffset: str("-0.0002568")},
		{name: "positive tie rounds away from zero", offset: f64(0.00000015), wantOffset: str("0.0000002")},
		{name: "negative tie rounds away from zero", offset: f64(-0.00000015), wantOffset: str("-0.0000002")},
		{name: "largest observed offset", offset: f64(-3.94e9), wantOffset: str("-3940000000")},
		{name: "offset inside Decimal64(7) range", offset: f64(9.9e10), wantOffset: str("99000000000")},
		{name: "offset at Decimal64(7) limit is NULL", offset: f64(1e11)},
		{name: "NaN offset is NULL", offset: f64(math.NaN())},
		{name: "+Inf offset is NULL", offset: f64(math.Inf(1))},
		{name: "-Inf offset is NULL", offset: f64(math.Inf(-1))},
		{
			name: "rtt, leap and error set", offset: f64(0.05), rtt: i64(150),
			leap: 1, lsError: "i/o timeout", wantOffset: str("0.05"),
		},
	}

	// Server IDs unique to this run, so re-running against a server left
	// up by "start" doesn't match rows from an earlier run.
	base := 1_000_000_000 + time.Now().UnixMilli()%1_000_000_000
	ts := time.Date(2026, 9, 14, 12, 34, 56, 0, time.UTC)

	logscores := make([]*logscore.LogScore, len(tests))
	for i, tt := range tests {
		logscores[i] = &logscore.LogScore{
			ID:        int64(i + 1),
			ServerID:  base + int64(i),
			MonitorID: 42,
			Ts:        ts.Unix(),
			Score:     19.5,
			Step:      -0.25,
			Offset:    tt.offset,
			RTT:       tt.rtt,
			Meta:      logscore.LogScoreMetadata{Leap: tt.leap, Error: tt.lsError},
		}
	}

	n, err := archiver.Store(logscores)
	require.NoError(t, err)
	require.Equal(t, len(logscores), n)

	_, err = db.Exec("SYSTEM FLUSH DISTRIBUTED log_scores")
	require.NoError(t, err)

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverID := uint32(base + int64(i))

			var count uint64
			err := db.QueryRow("SELECT count() FROM log_scores WHERE server_id = ?", serverID).Scan(&count)
			require.NoError(t, err)
			require.Equal(t, uint64(1), count, "row must be stored exactly once")

			var (
				monitorID   uint32
				gotTs, dt   time.Time
				score, step float32
				offset      sql.NullString
				rtt         *uint32
				leap        *uint8
				errorIsNull uint8
				lsError     string
			)
			// NULL-ness of error is checked server-side rather than through
			// the driver's scan of LowCardinality(Nullable(String)).
			err = db.QueryRow(`
				SELECT monitor_id, ts, dt, score, step, toString(offset), rtt, leap,
				       isNull(error), ifNull(error, '')
				FROM log_scores WHERE server_id = ?`, serverID).
				Scan(&monitorID, &gotTs, &dt, &score, &step, &offset, &rtt, &leap, &errorIsNull, &lsError)
			require.NoError(t, err)

			assert.Equal(t, uint32(42), monitorID)
			assert.Equal(t, ts.Unix(), gotTs.Unix())
			assert.Equal(t, "2026-09-14", dt.Format(time.DateOnly))
			assert.Equal(t, float32(19.5), score)
			assert.Equal(t, float32(-0.25), step)

			if tt.wantOffset == nil {
				assert.False(t, offset.Valid, "offset should be NULL, got %q", offset.String)
			} else {
				assert.True(t, offset.Valid, "offset should not be NULL")
				assert.Equal(t, *tt.wantOffset, offset.String)
			}

			if tt.rtt == nil {
				assert.Nil(t, rtt)
			} else if assert.NotNil(t, rtt) {
				assert.Equal(t, uint32(*tt.rtt), *rtt)
			}

			if tt.leap == 0 {
				assert.Nil(t, leap)
			} else if assert.NotNil(t, leap) {
				assert.Equal(t, tt.leap, *leap)
			}

			if tt.lsError == "" {
				assert.Equal(t, uint8(1), errorIsNull, "error should be NULL, got %q", lsError)
			} else {
				assert.Equal(t, uint8(0), errorIsNull, "error should not be NULL")
				assert.Equal(t, tt.lsError, lsError)
			}
		})
	}
}

// requireLoopback stops the test from writing rows into a real database if
// TEST_CLICKHOUSE_DSN is pointed somewhere other than a local server.
func requireLoopback(t *testing.T, dsn string) {
	t.Helper()
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	host := u.Hostname()
	if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		t.Fatalf("refusing to write test rows to non-local ClickHouse host %q", host)
	}
}
