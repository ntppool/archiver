package main

import (
	"fmt"
	"strings"

	"go.ntppool.org/common/database/pgdb"
	"go.ntppool.org/common/version"
)

// VersionCmd represents the version command
type VersionCmd struct{}

// Run executes the version command
func (cmd *VersionCmd) Run() error {
	fmt.Printf("archiver %s\n", version.Version())

	// Print source database configuration
	printSourceDBConfig()

	// Print configured storage backends with their primary attributes
	if globalConfig.Storage.ClickHouseDSN != "" {
		fmt.Println("\nStorage: ClickHouse")
		printClickHouseConfig(globalConfig.Storage.ClickHouseDSN)
	}
	if globalConfig.Storage.BigQueryDataset != "" {
		fmt.Println("\nStorage: BigQuery")
		fmt.Printf("  Dataset: %s\n", globalConfig.Storage.BigQueryDataset)
		fmt.Printf("  Project: %s\n", globalConfig.Storage.BigQueryProject)
	}
	if globalConfig.Storage.GCSBucket != "" {
		fmt.Println("\nStorage: Google Cloud Storage")
		fmt.Printf("  Bucket: %s\n", globalConfig.Storage.GCSBucket)
		fmt.Printf("  Project: %s\n", globalConfig.Storage.GCSProject)
	}
	if globalConfig.Storage.AvroPath != "" {
		fmt.Println("\nStorage: Local Avro")
		fmt.Printf("  Path: %s\n", globalConfig.Storage.AvroPath)
	}

	return nil
}

func printClickHouseConfig(dsn string) {
	// Parse ClickHouse DSN
	// Format: tcp://hostname:port/database?params
	dsn = strings.TrimPrefix(dsn, "tcp://")
	dsn = strings.TrimPrefix(dsn, "http://")
	dsn = strings.TrimPrefix(dsn, "https://")

	parts := strings.Split(dsn, "/")
	if len(parts) > 0 {
		fmt.Printf("  Host: %s\n", parts[0])
	}
	if len(parts) > 1 {
		dbPart := parts[1]
		// Remove query params
		dbPart = strings.Split(dbPart, "?")[0]
		fmt.Printf("  Database: %s\n", dbPart)
	}
}

func printSourceDBConfig() {
	fmt.Println("\nSource: PostgreSQL")

	// Try DATABASE_URI first
	if cfg, err := pgdb.ParseURIConfig(); err == nil && cfg != nil {
		fmt.Printf("  Host: %s\n", cfg.ConnConfig.Host)
		fmt.Printf("  Database: %s\n", cfg.ConnConfig.Database)
		return
	}

	// Try config files
	pgCfg, configFile, err := pgdb.FindConfig(pgdb.GetConfigFiles())
	if err != nil {
		fmt.Println("  (configuration not found)")
		return
	}
	if pgCfg != nil {
		fmt.Printf("  Host: %s\n", pgCfg.Host)
		fmt.Printf("  Database: %s\n", pgCfg.Name)
		fmt.Printf("  Config: %s\n", configFile)
	}
}
