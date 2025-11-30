package source

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
