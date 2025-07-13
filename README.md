# Log Score Archiver

    go get -u -v go.ntppool.org/archiver

    docker: askbjoernhansen/ntppool-archiver:1.0

## Architecture

The archiver uses a **connection pool architecture** with dynamic configuration support through `go.ntppool.org/common/database`. This provides:

- **Dynamic configuration** - Runtime database parameter changes without restart
- **Connection pooling** - Efficient connection management with health monitoring
- **Prometheus metrics** - Built-in monitoring of connection pool utilization
- **Context-aware operations** - Proper cancellation and timeout handling

## Configuration

The archiver uses environment variables for configuration with built-in validation. Configuration is managed through the Kong library which provides comprehensive validation and error reporting.

### Required Environment Variables

#### MySQL Database Connection
- `db_host` - MySQL host (e.g., `10.43.173.158`)
- `db_database` - Database name (e.g., `askntp`)
- `db_user` - Database username
- `db_pass` - Database password

### Optional Configuration

#### Database Connection Pool
- `db_max_idle_conns` - Maximum idle connections (default: 10)
- `db_max_open_conns` - Maximum open connections (default: 10)
- `db_max_idle_time` - Maximum idle connection time (default: 2m)
- `db_max_lifetime` - Maximum connection lifetime (default: 5m)
- `db_timeout` - Connection timeout (default: 10s)

**Dynamic Configuration**: The connection pool supports runtime configuration updates through the `UpdateConfig()` method, allowing database parameters to be changed without service restart.

#### Application Settings
- `retention_days` - Data retention period in days (default: 15)
- `app_valid_tables` - Comma-separated list of valid table names (default: `log_scores,log_scores_archive,log_scores_test`)

## Monitoring

The archiver includes built-in Prometheus metrics for monitoring:

- **Connection pool metrics** - Open, idle, and in-use connections
- **Query performance** - Connection wait times and durations  
- **Health monitoring** - Database connectivity status

Metrics are automatically registered with `prometheus.DefaultRegisterer` when the connection pool is initialized.

## Storage Backends

At least one storage backend must be configured. The archiver supports multiple backends running simultaneously.

### ClickHouse
- `ch_dsn` - ClickHouse connection string (e.g., `tcp://10.43.92.221:9000/askntp?debug=false&compress=lz4`)

### Google BigQuery
- `bq_dataset` - BigQuery dataset name
- `GOOGLE_APPLICATION_CREDENTIALS` - Path to service account key file (e.g., `keys/ntpdev-ask.json`)

### Google Cloud Storage (GCS)
- `gc_bucket` - GCS bucket name for storing Avro files
- `GOOGLE_APPLICATION_CREDENTIALS` - Path to service account key file

### Local Avro Files
- `avro_path` - Local directory path for Avro files (e.g., `/tmp/avro-data`)

## TODO

InfluxDB?

Deleting old log_scores older than X when all archivers have caught up.

delete
  from log_scores
  where
    ts < date_sub(now(), interval 32 day)
    and id < (select min(log_score_id) from log_scores_archive_status)
  order by id
  limit 10000000;

Status API (for monitoring)

Separate "Get data" API to pull data from the various archives (?)
