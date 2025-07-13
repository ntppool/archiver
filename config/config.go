package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/alecthomas/kong"
)

// Config holds all configuration for the archiver
type Config struct {
	// Storage configuration
	Storage Storage `embed:"" group:"Storage Configuration:"`

	// Application configuration
	App App `embed:"" group:"Application Configuration:"`

	// Batch configuration
	Batch Batch `embed:"" group:"Batch Configuration:"`

	// Cleanup configuration
	Cleanup Cleanup `embed:"" group:"Cleanup Configuration:"`
}


// Storage configuration for all backends
type Storage struct {
	// ClickHouse
	ClickHouseDSN string `env:"ch_dsn" help:"ClickHouse database connection string"`

	// BigQuery
	BigQueryDataset string `env:"bq_dataset" help:"BigQuery dataset name"`
	BigQueryProject string `env:"bq_project" default:"ntppool" help:"BigQuery project ID"`

	// Google Cloud Storage
	GCSBucket       string `env:"gc_bucket" help:"Google Cloud Storage bucket name"`
	GCSProject      string `env:"gc_project" default:"ntppool" help:"Google Cloud Storage project ID"`
	GCSContentType  string `env:"gc_content_type" default:"avro/binary" help:"GCS content type for uploads"`
	GCSCacheControl string `env:"gc_cache_control" default:"public, max-age=157248000" help:"GCS cache control header"`

	// Local Avro
	AvroPath string `env:"avro_path" help:"Local directory path for Avro files"`

	// Google Application Credentials
	GoogleApplicationCredentials string `env:"GOOGLE_APPLICATION_CREDENTIALS" help:"Path to Google service account credentials"`
}

// App configuration
type App struct {
	Version              string   `env:"app_version" default:"1.3" help:"Application version"`
	DefaultTable         string   `env:"app_default_table" default:"log_scores" help:"Default table name"`
	ValidTables          []string `env:"app_valid_tables" default:"log_scores,log_scores_archive,log_scores_test" help:"Valid table names (comma-separated)"`
	RetentionDays        int      `env:"retention_days" default:"15" help:"Data retention period in days"`
	RetentionDaysDefault int      `env:"retention_days_default" default:"14" help:"Default retention days fallback"`
}

// Batch configuration for different storage backends
type Batch struct {
	// BigQuery
	BigQueryMinSize  int           `env:"batch_bq_min_size" default:"200" help:"BigQuery minimum batch size"`
	BigQueryMaxSize  int           `env:"batch_bq_max_size" default:"10000000" help:"BigQuery maximum batch size"`
	BigQueryInterval time.Duration `env:"batch_bq_interval" default:"10m" help:"BigQuery batch processing interval"`

	// ClickHouse
	ClickHouseMinSize  int           `env:"batch_ch_min_size" default:"50" help:"ClickHouse minimum batch size"`
	ClickHouseMaxSize  int           `env:"batch_ch_max_size" default:"500000" help:"ClickHouse maximum batch size"`
	ClickHouseInterval time.Duration `env:"batch_ch_interval" default:"0ms" help:"ClickHouse batch processing interval"`

	// File Avro
	FileAvroMinSize    int           `env:"batch_avro_min_size" default:"500000" help:"File Avro minimum batch size"`
	FileAvroMaxSize    int           `env:"batch_avro_max_size" default:"10000000" help:"File Avro maximum batch size"`
	FileAvroInterval   time.Duration `env:"batch_avro_interval" default:"24h" help:"File Avro batch processing interval"`
	FileAvroAppendSize int           `env:"batch_avro_append_size" default:"50000" help:"File Avro append batch size"`
}

// Cleanup configuration
type Cleanup struct {
	DefaultInterval time.Duration `env:"cleanup_default_interval" default:"4m" help:"Default cleanup interval"`
	BatchSize       int           `env:"cleanup_batch_size" default:"200000" help:"Cleanup batch size"`
	ReducedInterval time.Duration `env:"cleanup_reduced_interval" default:"1m" help:"Reduced cleanup interval when batch is full"`
	FakeInterval    time.Duration `env:"cleanup_fake_interval" default:"10m" help:"Fake cleanup archiver interval"`
}

// Validate validates the configuration
func (c *Config) Validate() error {
	// Validate storage configuration - at least one backend must be configured
	hasStorage := false
	if c.Storage.ClickHouseDSN != "" {
		hasStorage = true
	}
	if c.Storage.BigQueryDataset != "" {
		hasStorage = true
	}
	if c.Storage.GCSBucket != "" {
		hasStorage = true
	}
	if c.Storage.AvroPath != "" {
		hasStorage = true
	}

	if !hasStorage {
		return fmt.Errorf("at least one storage backend must be configured (ch_dsn, bq_dataset, gc_bucket, or avro_path)")
	}

	// Validate app configuration
	if c.App.DefaultTable == "" {
		return fmt.Errorf("default table (app_default_table) is required")
	}
	if len(c.App.ValidTables) == 0 {
		return fmt.Errorf("valid tables list (app_valid_tables) cannot be empty")
	}
	if c.App.RetentionDays <= 0 {
		return fmt.Errorf("retention days must be positive")
	}

	// Validate batch configuration
	if c.Batch.BigQueryMinSize <= 0 || c.Batch.BigQueryMaxSize <= 0 {
		return fmt.Errorf("BigQuery batch sizes must be positive")
	}
	if c.Batch.ClickHouseMinSize <= 0 || c.Batch.ClickHouseMaxSize <= 0 {
		return fmt.Errorf("ClickHouse batch sizes must be positive")
	}
	if c.Batch.FileAvroMinSize <= 0 || c.Batch.FileAvroMaxSize <= 0 {
		return fmt.Errorf("File Avro batch sizes must be positive")
	}

	return nil
}

// PostProcess performs post-processing on the configuration after Kong parsing
func (c *Config) PostProcess() error {
	// Convert comma-separated valid tables to slice if needed
	if len(c.App.ValidTables) == 1 && strings.Contains(c.App.ValidTables[0], ",") {
		c.App.ValidTables = strings.Split(c.App.ValidTables[0], ",")
		for i, table := range c.App.ValidTables {
			c.App.ValidTables[i] = strings.TrimSpace(table)
		}
	}

	return c.Validate()
}


// IsValidTable checks if a table name is in the valid tables list
func (c *Config) IsValidTable(table string) bool {
	for _, validTable := range c.App.ValidTables {
		if validTable == table {
			return true
		}
	}
	return false
}

// GetLockName returns the lock name for a given table
func (c *Config) GetLockName(table string) string {
	return "archiver-" + table
}

// LoadGlobalConfig loads the configuration from environment variables
// This is used when we need config in places that don't have access to the global config
func LoadGlobalConfig() (*Config, error) {
	var cfg Config

	// Parse with Kong to get environment variables
	parser, err := kong.New(&cfg)
	if err != nil {
		return nil, fmt.Errorf("creating parser: %w", err)
	}

	// Parse empty args to load from environment
	_, err = parser.Parse([]string{})
	if err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	// Post-process and validate
	if err := cfg.PostProcess(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}
