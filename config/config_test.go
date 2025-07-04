package config

import (
	"os"
	"testing"
	"time"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKongConfig(t *testing.T) {
	// Set up minimal required environment variables
	os.Setenv("db_host", "localhost")
	os.Setenv("db_database", "testdb")
	os.Setenv("db_user", "testuser")
	os.Setenv("db_pass", "testpass")
	os.Setenv("avro_path", "/tmp/test")

	defer func() {
		os.Unsetenv("db_host")
		os.Unsetenv("db_database")
		os.Unsetenv("db_user")
		os.Unsetenv("db_pass")
		os.Unsetenv("avro_path")
	}()

	var cfg Config
	parser, err := kong.New(&cfg)
	require.NoError(t, err)

	// Parse configuration (without command line args)
	_, err = parser.Parse([]string{})
	require.NoError(t, err)

	// Post-process the configuration
	err = cfg.PostProcess()
	require.NoError(t, err)

	// Test database configuration
	assert.Equal(t, "localhost", cfg.Database.Host)
	assert.Equal(t, "testdb", cfg.Database.Database)
	assert.Equal(t, "testuser", cfg.Database.User)
	assert.Equal(t, "testpass", cfg.Database.Password)

	// Test defaults
	assert.Equal(t, "utf8mb4", cfg.Database.Charset)
	assert.Equal(t, "UTC", cfg.Database.TimeZone)
	assert.Equal(t, 10*time.Second, cfg.Database.Timeout)
	assert.Equal(t, 2*time.Minute, cfg.Database.MaxIdleTime)
	assert.Equal(t, 5*time.Minute, cfg.Database.MaxLifetime)
	assert.Equal(t, 10, cfg.Database.MaxIdleConns)
	assert.Equal(t, 10, cfg.Database.MaxOpenConns)
	assert.Equal(t, "archiver-", cfg.Database.LockNamePrefix)
	assert.Equal(t, 0, cfg.Database.LockTimeout)
	assert.True(t, cfg.Database.ParseTime)
	assert.True(t, cfg.Database.RejectReadOnly)

	// Test storage configuration
	assert.Equal(t, "/tmp/test", cfg.Storage.AvroPath)
	assert.Equal(t, "ntppool", cfg.Storage.BigQueryProject)
	assert.Equal(t, "ntppool", cfg.Storage.GCSProject)
	assert.Equal(t, "avro/binary", cfg.Storage.GCSContentType)
	assert.Equal(t, "public, max-age=157248000", cfg.Storage.GCSCacheControl)

	// Test app configuration
	assert.Equal(t, "1.3", cfg.App.Version)
	assert.Equal(t, "log_scores", cfg.App.DefaultTable)
	assert.Equal(t, []string{"log_scores", "log_scores_archive", "log_scores_test"}, cfg.App.ValidTables)
	assert.Equal(t, 15, cfg.App.RetentionDays)
	assert.Equal(t, 14, cfg.App.RetentionDaysDefault)

	// Test batch configuration
	assert.Equal(t, 200, cfg.Batch.BigQueryMinSize)
	assert.Equal(t, 10000000, cfg.Batch.BigQueryMaxSize)
	assert.Equal(t, 10*time.Minute, cfg.Batch.BigQueryInterval)
	assert.Equal(t, 50, cfg.Batch.ClickHouseMinSize)
	assert.Equal(t, 500000, cfg.Batch.ClickHouseMaxSize)
	assert.Equal(t, 0*time.Millisecond, cfg.Batch.ClickHouseInterval)
	assert.Equal(t, 500000, cfg.Batch.FileAvroMinSize)
	assert.Equal(t, 10000000, cfg.Batch.FileAvroMaxSize)
	assert.Equal(t, 24*time.Hour, cfg.Batch.FileAvroInterval)
	assert.Equal(t, 50000, cfg.Batch.FileAvroAppendSize)

	// Test cleanup configuration
	assert.Equal(t, 4*time.Minute, cfg.Cleanup.DefaultInterval)
	assert.Equal(t, 200000, cfg.Cleanup.BatchSize)
	assert.Equal(t, 1*time.Minute, cfg.Cleanup.ReducedInterval)
	assert.Equal(t, 10*time.Minute, cfg.Cleanup.FakeInterval)
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: &Config{
				Database: Database{
					Host:     "localhost",
					Database: "testdb",
					User:     "testuser",
					Password: "testpass",
				},
				Storage: Storage{
					AvroPath: "/tmp/test",
				},
				App: App{
					DefaultTable:  "log_scores",
					ValidTables:   []string{"log_scores"},
					RetentionDays: 15,
				},
				Batch: Batch{
					BigQueryMinSize:   200,
					BigQueryMaxSize:   10000000,
					ClickHouseMinSize: 50,
					ClickHouseMaxSize: 500000,
					FileAvroMinSize:   500000,
					FileAvroMaxSize:   10000000,
				},
			},
			wantErr: false,
		},
		{
			name: "no storage backend",
			config: &Config{
				Database: Database{
					Host:     "localhost",
					Database: "testdb",
					User:     "testuser",
					Password: "testpass",
				},
				Storage: Storage{},
				App: App{
					DefaultTable:  "log_scores",
					ValidTables:   []string{"log_scores"},
					RetentionDays: 15,
				},
				Batch: Batch{
					BigQueryMinSize:   200,
					BigQueryMaxSize:   10000000,
					ClickHouseMinSize: 50,
					ClickHouseMaxSize: 500000,
					FileAvroMinSize:   500000,
					FileAvroMaxSize:   10000000,
				},
			},
			wantErr: true,
			errMsg:  "at least one storage backend must be configured",
		},
		{
			name: "invalid retention days",
			config: &Config{
				Database: Database{
					Host:     "localhost",
					Database: "testdb",
					User:     "testuser",
					Password: "testpass",
				},
				Storage: Storage{
					AvroPath: "/tmp/test",
				},
				App: App{
					DefaultTable:  "log_scores",
					ValidTables:   []string{"log_scores"},
					RetentionDays: -1,
				},
				Batch: Batch{
					BigQueryMinSize:   200,
					BigQueryMaxSize:   10000000,
					ClickHouseMinSize: 50,
					ClickHouseMaxSize: 500000,
					FileAvroMinSize:   500000,
					FileAvroMaxSize:   10000000,
				},
			},
			wantErr: true,
			errMsg:  "retention days must be positive",
		},
		{
			name: "invalid batch size",
			config: &Config{
				Database: Database{
					Host:     "localhost",
					Database: "testdb",
					User:     "testuser",
					Password: "testpass",
				},
				Storage: Storage{
					AvroPath: "/tmp/test",
				},
				App: App{
					DefaultTable:  "log_scores",
					ValidTables:   []string{"log_scores"},
					RetentionDays: 15,
				},
				Batch: Batch{
					BigQueryMinSize:   0,
					BigQueryMaxSize:   10000000,
					ClickHouseMinSize: 50,
					ClickHouseMaxSize: 500000,
					FileAvroMinSize:   500000,
					FileAvroMaxSize:   10000000,
				},
			},
			wantErr: true,
			errMsg:  "BigQuery batch sizes must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGetMySQLDSN(t *testing.T) {
	cfg := &Config{
		Database: Database{
			Host:           "localhost",
			Database:       "testdb",
			User:           "testuser",
			Password:       "testpass",
			Charset:        "utf8mb4",
			TimeZone:       "UTC",
			Timeout:        10 * time.Second,
			ParseTime:      true,
			RejectReadOnly: true,
		},
	}

	dsn := cfg.GetMySQLDSN()
	expected := "testuser:testpass@tcp(localhost)/testdb?parseTime=true&charset=utf8mb4&rejectReadOnly=true&timeout=10s&loc=UTC"
	assert.Equal(t, expected, dsn)
}

func TestIsValidTable(t *testing.T) {
	cfg := &Config{
		App: App{
			ValidTables: []string{"log_scores", "log_scores_archive", "log_scores_test"},
		},
	}

	assert.True(t, cfg.IsValidTable("log_scores"))
	assert.True(t, cfg.IsValidTable("log_scores_archive"))
	assert.True(t, cfg.IsValidTable("log_scores_test"))
	assert.False(t, cfg.IsValidTable("invalid_table"))
	assert.False(t, cfg.IsValidTable(""))
}

func TestGetLockName(t *testing.T) {
	cfg := &Config{
		Database: Database{
			LockNamePrefix: "archiver-",
		},
	}

	assert.Equal(t, "archiver-log_scores", cfg.GetLockName("log_scores"))
	assert.Equal(t, "archiver-test", cfg.GetLockName("test"))
}

func TestPostProcess(t *testing.T) {
	cfg := &Config{
		Database: Database{
			Host:     "localhost",
			Database: "testdb",
			User:     "testuser",
			Password: "testpass",
		},
		Storage: Storage{
			AvroPath: "/tmp/test",
		},
		App: App{
			DefaultTable:  "log_scores",
			ValidTables:   []string{"log_scores,log_scores_archive,log_scores_test"},
			RetentionDays: 15,
		},
		Batch: Batch{
			BigQueryMinSize:   200,
			BigQueryMaxSize:   10000000,
			ClickHouseMinSize: 50,
			ClickHouseMaxSize: 500000,
			FileAvroMinSize:   500000,
			FileAvroMaxSize:   10000000,
		},
	}

	err := cfg.PostProcess()
	require.NoError(t, err)

	// Should have split the comma-separated string
	assert.Equal(t, []string{"log_scores", "log_scores_archive", "log_scores_test"}, cfg.App.ValidTables)
}

func TestSpecialEnvironmentVariables(t *testing.T) {
	// Test GOOGLE_APPLICATION_CREDENTIALS
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/path/to/creds.json")
	os.Setenv("retention_days", "30")
	os.Setenv("db_host", "localhost")
	os.Setenv("db_database", "testdb")
	os.Setenv("db_user", "testuser")
	os.Setenv("db_pass", "testpass")
	os.Setenv("avro_path", "/tmp/test")

	defer func() {
		os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS")
		os.Unsetenv("retention_days")
		os.Unsetenv("db_host")
		os.Unsetenv("db_database")
		os.Unsetenv("db_user")
		os.Unsetenv("db_pass")
		os.Unsetenv("avro_path")
	}()

	var cfg Config
	parser, err := kong.New(&cfg)
	require.NoError(t, err)

	// Parse configuration (without command line args)
	_, err = parser.Parse([]string{})
	require.NoError(t, err)

	// Post-process the configuration
	err = cfg.PostProcess()
	require.NoError(t, err)

	assert.Equal(t, "/path/to/creds.json", cfg.Storage.GoogleApplicationCredentials)
	assert.Equal(t, 30, cfg.App.RetentionDays)
}
