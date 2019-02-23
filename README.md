# Log Score Archiver

docker: askbjoernhansen/ntppool-archiver:1.0

## Sorage backends

- ClickHouse
- Avro files
- Avro files in GCS

## TODO

InfluxDB?

Load into BigQuery

Deleting old log_scores older than X when all archivers have caught up.

Status API (for monitoring)

Separate "Get data" API to pull data from the various archives (?)
