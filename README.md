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

The archiver uses **file-based configuration** for database settings and environment variables for application settings. Configuration is managed through the Kong library with built-in validation.

### Database Configuration

The archiver uses `go.ntppool.org/common/database` for database configuration. Database settings are read from:

1. **Primary**: `database.yaml` (current directory)
2. **Secondary**: `/vault/secrets/database.yaml` (Kubernetes secrets)
3. **Fallback**: `DATABASE_DSN` environment variable

#### database.yaml Example
```yaml
mysql:
  dsn: "user:password@tcp(host:port)/database?parseTime=true&charset=utf8mb4"
  user: "optional_user_override"
  pass: "optional_password_override"
  dbname: "optional_database_override"
```

#### Environment Variable Fallback
If no `database.yaml` file is found, set:
```bash
export DATABASE_DSN="user:password@tcp(host:port)/database?parseTime=true&charset=utf8mb4"
```

#### Connection Pool Settings
- **Max Open Connections**: 25
- **Max Idle Connections**: 10
- **Connection Lifetime**: 3 minutes
- **Prometheus Metrics**: Enabled by default

**Dynamic Configuration**: The connection pool supports runtime configuration updates through the `UpdateConfig()` method by re-reading configuration files.

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
