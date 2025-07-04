package fileavro

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.ntppool.org/archiver/logscore"
)

func TestNewArchiver(t *testing.T) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "fileavro_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a temporary file (not a directory)
	tempFile := filepath.Join(tempDir, "testfile")
	file, err := os.Create(tempFile)
	require.NoError(t, err)
	file.Close()

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{
			name:    "valid directory",
			path:    tempDir,
			wantErr: false,
		},
		{
			name:    "nonexistent path",
			path:    "/nonexistent/path",
			wantErr: true,
		},
		{
			name:    "path is a file, not directory",
			path:    tempFile,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			archiver, err := NewArchiver(tt.path)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, archiver)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, archiver)
				assert.Equal(t, tt.path, archiver.(*AvroArchiver).path)
			}
		})
	}
}

func TestBatchSizeMinMaxTime(t *testing.T) {
	archiver := &AvroArchiver{path: "/tmp"}
	minSize, maxSize, interval := archiver.BatchSizeMinMaxTime()
	assert.Equal(t, 500000, minSize)
	assert.Equal(t, 10000000, maxSize)
	assert.Equal(t, time.Hour*24, interval)
}

func TestFileName(t *testing.T) {
	archiver := &AvroArchiver{path: "/tmp"}

	tests := []struct {
		name       string
		logscores  []*logscore.LogScore
		wantResult string
	}{
		{
			name:       "empty logscores",
			logscores:  []*logscore.LogScore{},
			wantResult: "",
		},
		{
			name: "single logscore",
			logscores: []*logscore.LogScore{
				{ID: 123, Ts: 1640995200},
			},
			wantResult: "1640995200-123.avro",
		},
		{
			name: "multiple logscores",
			logscores: []*logscore.LogScore{
				{ID: 123, Ts: 1640995200},
				{ID: 124, Ts: 1640995260},
			},
			wantResult: "1640995200-123.avro", // Should use first record
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := archiver.FileName(tt.logscores)
			assert.Equal(t, tt.wantResult, result)
		})
	}
}

func TestClose(t *testing.T) {
	archiver := &AvroArchiver{path: "/tmp"}
	err := archiver.Close()
	assert.NoError(t, err)
}

func TestStoreWriter(t *testing.T) {
	archiver := &AvroArchiver{path: "/tmp"}

	t.Run("empty logscores", func(t *testing.T) {
		var buf bytes.Buffer
		count, err := archiver.StoreWriter(&buf, []*logscore.LogScore{})
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("single logscore", func(t *testing.T) {
		var buf bytes.Buffer
		
		offset := 0.05
		rtt := int64(150)
		logscores := []*logscore.LogScore{
			{
				ID:        123,
				ServerID:  20,
				MonitorID: 10,
				Ts:        1640995200,
				Score:     15.5,
				Step:      0.1,
				Offset:    &offset,
				RTT:       &rtt,
				Meta: logscore.LogScoreMetadata{
					Leap:  1,
					Error: "test error",
				},
			},
		}

		count, err := archiver.StoreWriter(&buf, logscores)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.Greater(t, buf.Len(), 0, "Buffer should contain avro data")
	})

	t.Run("multiple logscores", func(t *testing.T) {
		var buf bytes.Buffer
		
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
			{
				ID:        124,
				ServerID:  21,
				MonitorID: 11,
				Ts:        1640995260,
				Score:     16.0,
				Step:      0.2,
				Offset:    nil,
				RTT:       nil,
				Meta:      logscore.LogScoreMetadata{},
			},
		}

		count, err := archiver.StoreWriter(&buf, logscores)
		assert.NoError(t, err)
		assert.Equal(t, 2, count)
		assert.Greater(t, buf.Len(), 0, "Buffer should contain avro data")
	})

	t.Run("logscores with null values", func(t *testing.T) {
		var buf bytes.Buffer
		
		logscores := []*logscore.LogScore{
			{
				ID:        123,
				ServerID:  20,
				MonitorID: 10,
				Ts:        1640995200,
				Score:     15.5,
				Step:      0.1,
				Offset:    nil, // null offset
				RTT:       nil, // null rtt
				Meta:      logscore.LogScoreMetadata{}, // empty meta
			},
		}

		count, err := archiver.StoreWriter(&buf, logscores)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.Greater(t, buf.Len(), 0, "Buffer should contain avro data")
	})
}

func TestStoreWriterBatching(t *testing.T) {
	archiver := &AvroArchiver{path: "/tmp"}

	t.Run("large batch exceeds batchAppendSize", func(t *testing.T) {
		var buf bytes.Buffer
		
		// Create more than batchAppendSize (50000) records
		logscores := make([]*logscore.LogScore, batchAppendSize+10)
		for i := range logscores {
			logscores[i] = &logscore.LogScore{
				ID:        int64(i + 1),
				ServerID:  int64(20 + i),
				MonitorID: int64(10 + i),
				Ts:        int64(1640995200 + i),
				Score:     float64(15.5 + float64(i)*0.1),
				Step:      0.1,
				Offset:    nil,
				RTT:       nil,
				Meta:      logscore.LogScoreMetadata{},
			}
		}

		count, err := archiver.StoreWriter(&buf, logscores)
		assert.NoError(t, err)
		assert.Equal(t, len(logscores), count)
		assert.Greater(t, buf.Len(), 0, "Buffer should contain avro data")
	})
}

func TestStore(t *testing.T) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "fileavro_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	archiver, err := NewArchiver(tempDir)
	require.NoError(t, err)

	t.Run("empty logscores", func(t *testing.T) {
		count, err := archiver.Store([]*logscore.LogScore{})
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("single logscore", func(t *testing.T) {
		offset := 0.05
		rtt := int64(150)
		logscores := []*logscore.LogScore{
			{
				ID:        123,
				ServerID:  20,
				MonitorID: 10,
				Ts:        1640995200,
				Score:     15.5,
				Step:      0.1,
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

		// Check that file was created
		fileName := archiver.(*AvroArchiver).FileName(logscores)
		filePath := filepath.Join(tempDir, fileName)
		assert.FileExists(t, filePath)

		// Check file is not empty
		fileInfo, err := os.Stat(filePath)
		assert.NoError(t, err)
		assert.Greater(t, fileInfo.Size(), int64(0))
	})

	t.Run("multiple logscores", func(t *testing.T) {
		logscores := []*logscore.LogScore{
			{
				ID:        200,
				ServerID:  30,
				MonitorID: 20,
				Ts:        1640995300,
				Score:     20.5,
				Step:      0.2,
				Offset:    nil,
				RTT:       nil,
				Meta:      logscore.LogScoreMetadata{},
			},
			{
				ID:        201,
				ServerID:  31,
				MonitorID: 21,
				Ts:        1640995360,
				Score:     21.0,
				Step:      0.3,
				Offset:    nil,
				RTT:       nil,
				Meta:      logscore.LogScoreMetadata{},
			},
		}

		count, err := archiver.Store(logscores)
		assert.NoError(t, err)
		assert.Equal(t, 2, count)

		// Check that file was created
		fileName := archiver.(*AvroArchiver).FileName(logscores)
		filePath := filepath.Join(tempDir, fileName)
		assert.FileExists(t, filePath)

		// Check file is not empty
		fileInfo, err := os.Stat(filePath)
		assert.NoError(t, err)
		assert.Greater(t, fileInfo.Size(), int64(0))
	})

	// Test with original test data for regression
	t.Run("original test data", func(t *testing.T) {
		rtt := int64(11234)

		ls := []*logscore.LogScore{
			{
				ID:        103535350,
				ServerID:  200,
				MonitorID: 1,
				Ts:        1547999353,
				Score:     19.2,
				Step:      0.9,
				Offset:    nil,
				RTT:       &rtt,
				Meta:      logscore.LogScoreMetadata{Leap: 0},
			},
		}

		i, err := archiver.Store(ls)
		assert.NoError(t, err)
		assert.Equal(t, 1, i)
	})
}

func TestStoreFileError(t *testing.T) {
	// Test with a path that doesn't exist
	archiver := &AvroArchiver{path: "/nonexistent/path"}

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
	assert.Error(t, err)
	assert.Equal(t, 0, count)
	assert.Contains(t, err.Error(), "open file")
}

// mockReadWriter is a mock implementation of io.ReadWriter for testing error scenarios
type mockReadWriter struct {
	writeErr error
	readErr  error
}

func (m *mockReadWriter) Write(p []byte) (n int, err error) {
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	return len(p), nil
}

func (m *mockReadWriter) Read(p []byte) (n int, err error) {
	if m.readErr != nil {
		return 0, m.readErr
	}
	return 0, io.EOF
}

func TestStoreWriterError(t *testing.T) {
	archiver := &AvroArchiver{path: "/tmp"}

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

	t.Run("write error", func(t *testing.T) {
		mockWriter := &mockReadWriter{
			writeErr: assert.AnError,
		}

		count, err := archiver.StoreWriter(mockWriter, logscores)
		assert.Error(t, err)
		assert.Equal(t, 0, count)
	})
}
