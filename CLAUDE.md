# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Build & Test
- `go build -v -o archiver ./cmd/archiver/` - Build the archiver binary
- `GOOS=linux go build -v -o archiver_linux ./cmd/archiver/` - Build for Linux (see Makefile)
- `go test ./...` - Run all tests
- `go test ./storage/fileavro/` - Run specific package tests
- `gofumpt -w .` - Format code (required before commits)

### Running the Archiver
- `./archiver archive` - Run the archiver (default table: log_scores)
- `./archiver archive -t custom_table` - Archive from a specific table

### Local Development Setup
For local development, the easiest storage backends are:
- **File Avro**: Set `avro_path=/tmp/avro-data` to store Avro files locally
- **Local ClickHouse**: Set `ch_dsn` to your local ClickHouse instance

### Configuration Management
The archiver uses the Kong library for centralized configuration management. All configuration is loaded from environment variables with comprehensive validation.

#### Required Environment Variables

**MySQL Database Connection**:
- `db_host` - MySQL host (e.g., `10.43.173.158`)
- `db_database` - Database name (e.g., `askntp`)
- `db_user` - Database username
- `db_pass` - Database password

**Database Connection Pool** (optional, with defaults):
- `db_max_idle_conns=10` - Maximum idle connections
- `db_max_open_conns=10` - Maximum open connections
- `db_max_idle_time=2m` - Maximum idle connection time
- `db_max_lifetime=5m` - Maximum connection lifetime
- `db_timeout=10s` - Connection timeout

**Application Settings** (optional, with defaults):
- `retention_days=15` - Data retention period in days
- `app_valid_tables=log_scores,log_scores_archive,log_scores_test` - Valid table names

#### Storage Backend Configuration
At least one storage backend must be configured:

**ClickHouse**:
- `ch_dsn` - ClickHouse connection string (e.g., `tcp://10.43.92.221:9000/askntp?debug=false&compress=lz4`)

**BigQuery**:
- `bq_dataset` - BigQuery dataset name
- `GOOGLE_APPLICATION_CREDENTIALS` - Path to service account key file (e.g., `keys/ntpdev-ask.json`)

**Google Cloud Storage (GCS)**:
- `gc_bucket` - GCS bucket name for storing Avro files
- `GOOGLE_APPLICATION_CREDENTIALS` - Path to service account key file

**Local Avro Files**:
- `avro_path` - Local directory path for Avro files (e.g., `/tmp/avro-data`)

#### Other Configuration
- `retention_days` - Number of days to retain data (default: 15)

### Development Environment
- Environment files: `ntppool.*.env` (various environments: beta, prod, local)
- Configuration: Uses Viper for config management, supports `--config` flag
- Service keys: Stored in `keys/` directory for different environments

## Architecture

### Core Components
- **Main Application**: `cmd/archiver/` - CLI tool using Cobra framework
- **Storage Interface**: `storage/storage.go` - Defines `Archiver` interface with `Store()`, `BatchSizeMinMaxTime()`, `Close()` methods
- **Storage Backends**: Multiple implementations in `storage/` subdirectories
- **Data Models**: `logscore` package for log score data structures

### Storage Backends
The archiver supports multiple storage backends through a pluggable architecture:
- **BigQuery**: `storage/bigquery/` - Google BigQuery integration
- **ClickHouse**: `storage/clickhouse/` - ClickHouse database
- **File Avro**: `storage/fileavro/` - Local Avro files
- **GCS Avro**: `storage/gcsavro/` - Avro files in Google Cloud Storage
- **Cleanup**: `storage/cleanup/` - Data cleanup operations

Backend selection is handled by `SetupArchiver()` function in `archiver.go`.

### Key Dependencies
- `github.com/alecthomas/kong` - CLI framework and configuration management
- `cloud.google.com/go/bigquery` - BigQuery client
- `github.com/ClickHouse/clickhouse-go/v2` - ClickHouse driver
- `github.com/linkedin/goavro/v2` - Avro serialization
- `go.ntppool.org/common` - NTP Pool common utilities

### Database Integration
- Uses `go.ntppool.org/archiver/db` for database connections
- MySQL integration via `github.com/go-sql-driver/mysql` and `github.com/jmoiron/sqlx`
- Implements distributed locking using MySQL's `GET_LOCK()` function
- MySQL schema available at: https://raw.githubusercontent.com/ntppool/monitor/refs/heads/main/schema.sql
- ClickHouse schema documentation: TODO - needs to be extracted and documented

## Configuration
- **Kong-based Configuration**: Centralized configuration management with validation
- **Environment Variables**: All configuration loaded from environment variables
- **Built-in Validation**: Fail-fast startup with clear error messages for missing/invalid config
- **Storage Backend Detection**: Automatically detects and validates available storage backends
- **Multi-environment setup**: Different `.env` files for various environments
- **Service account keys**: Stored in `keys/` directory

## Deployment
- Linux builds via `make linux`
- Kubernetes deployment
- Docker image: `askbjoernhansen/ntppool-archiver:1.0`
- Drone CI integration with `drone sign` command