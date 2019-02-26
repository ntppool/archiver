# Log Score Archiver

    go get -u -v go.ntppool.org/archiver

    docker: askbjoernhansen/ntppool-archiver:1.0

## Sorage backends

- ClickHouse
- Google BigQuery
- Avro files
- Avro files stored in Google Cloud Storage

## TODO

InfluxDB?

Load into BigQuery

Deleting old log_scores older than X when all archivers have caught up.

Status API (for monitoring)

Separate "Get data" API to pull data from the various archives (?)
