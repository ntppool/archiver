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
