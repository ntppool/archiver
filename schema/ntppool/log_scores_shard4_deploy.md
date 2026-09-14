# Deploy log_scores_shard4 (storage reduction rewrite)

Covers `askntp`, `ntpbeta` and `ntppool`. Design and benchmarks:
`~/src/go/ntp/monitor/plans/2026-08-01-clickhouse-log-scores-compression.md`.

Migrate in order: **askntp first** (development), then `ntpbeta`, then
`ntppool`. The topology is identical in all three -- `log_scores`
(Distributed) over `log_scores_shard` (ReplicatedMergeTree) -- so there is no
per-database variation in the steps below beyond the database name.

Three changes matter to the writer (`storage/clickhouse/clickhouse.go`):

- `id` is dropped
- `dt` becomes `ALIAS toDate(ts)` and cannot be inserted into
- `offset` becomes `Nullable(Decimal64(7))`, so values must be **rounded**,
  not truncated (`toDecimal64` truncates toward zero, a systematic bias)

## No `ON CLUSTER` -- these are Replicated databases

`askntp`, `ntpbeta` and `ntppool` all use the Replicated database engine:

```
Replicated('/clickhouse/databases/ntppool', '{shard}', '{replica}')
```

The database engine propagates DDL itself, and `ON CLUSTER` is rejected:

```
Code: 80. DB::Exception: It's not initial query.
ON CLUSTER is not allowed for Replicated database. (INCORRECT_QUERY)
```

So every statement here is run **once, against one node**, with no
`ON CLUSTER` clause. This is also why the `schema/` files in the chgw repo
omit it.

The Distributed engine's first argument stays `'scores'`. That names the
cluster queries are routed across and is unrelated to how DDL replicates.

## Engine clause: explicit ZooKeeper path

The new tables use the same explicit form as the live `log_scores_shard`:

```sql
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
```

Under a Replicated database ClickHouse supplies exactly these values when the
arguments are omitted, so `ReplicatedMergeTree()` would be equivalent. The
explicit form is kept so the new table's engine clause matches the table it
replaces verbatim, which makes a `SHOW CREATE TABLE` diff during the swap
trivial to read.

**Confirm before creating**, since this was taken from a quoted
`SHOW CREATE TABLE` rather than a live query at authoring time:

```sql
SHOW CREATE TABLE ntppool.log_scores_shard;
```

If the live clause differs, match it and update these files.

## 0. Confirm the naming generation

`log_scores_shard4` was verified free in all three databases. `ntpbeta` also
carried an orphaned generation 3 (`log_scores3` -> `log_scores_shard3`) with
**zero parts** -- created and abandoned before any data was written, now
being dropped. `ntppool` and `askntp` were already clean. This confirms
`~/src/go/data-api/ddl-temp/{pool,beta}_scores.sql` describes dead tables and
should not be treated as current schema.

```sql
SHOW TABLES FROM askntp  LIKE 'log%';
SHOW TABLES FROM ntpbeta LIKE 'log%';
SHOW TABLES FROM ntppool LIKE 'log%';
```

The `CREATE TABLE` statements here deliberately omit `IF NOT EXISTS` so a
name collision fails loudly rather than backfilling into the wrong table.

## 1. Create the new local table

```sql
-- from schema/askntp/log_scores_shard4.sql
-- from schema/ntpbeta/log_scores_shard4.sql
-- from schema/ntppool/log_scores_shard4.sql
```

## 2. Backfill, one quarter at a time, oldest first

Run on one replica and let replication carry the parts. Peak extra disk
stays at one quarter, and the process is interruptible.

```sql
INSERT INTO ntppool.log_scores_shard4
SELECT monitor_id, server_id, ts, score, step,
       toDecimal64OrNull(toString(round(offset, 7)), 7),
       rtt, leap, warning, nullIf(error, '')
FROM ntppool.log_scores_shard
WHERE ts >= '<quarter start>' AND ts < '<quarter end>';
```

`dt` appears in neither the column list nor the SELECT -- it is an ALIAS.
`round()` before the cast is what removes the truncation bias.
`nullIf(error, '')` turns the `''` the old archiver wrote for "no error" into
NULL, which is what the fixed archiver writes.

Verify each quarter before dropping it from the old table. `no_error` must
match; `empty_error` must be 0 on the new table:

```sql
SELECT count(), countIf(offset IS NULL),
       countIf(error IS NULL OR error = '') AS no_error,
       countIf(error = '') AS empty_error
FROM ntppool.log_scores_shard
WHERE ts >= '<quarter start>' AND ts < '<quarter end>';

SELECT count(), countIf(offset IS NULL),
       countIf(error IS NULL OR error = '') AS no_error,
       countIf(error = '') AS empty_error
FROM ntppool.log_scores_shard4
WHERE ts >= '<quarter start>' AND ts < '<quarter end>';

ALTER TABLE ntppool.log_scores_shard DROP PARTITION '<quarter>';
```

## 3. Cut over

Stop the archiver first (`kubectl scale deploy/archiver --replicas=0`, or set
`replicas: 0` in flux). While it is stopped, rows accumulate in the PostgreSQL
`log_scores` table: the cleanup archiver only deletes below
`MIN(log_score_id)` from `log_scores_archive_status`, so nothing is lost --
the cost is PostgreSQL disk, roughly 15M rows/day on ntppool.

Copy the final tail (rows written since the last quarter backfill), then swap
the Distributed table:

```sql
-- from schema/askntp/log_scores.sql
-- from schema/ntpbeta/log_scores.sql
-- from schema/ntppool/log_scores.sql
```

A Distributed table holds no data, so `DROP TABLE` followed by the new
`CREATE` is safe; `EXCHANGE TABLES` also works. Neither takes `ON CLUSTER`.

## 4. Deploy the writer and reader

Only after the swap:

- archiver: stop sending `dt` and `id`, send `offset` as a `decimal.Decimal`
  rounded to 7 places, and send `error` as a nil `*string` when empty so it
  is stored as NULL (`storage/clickhouse/clickhouse.go`). Verified against a
  local server by `scripts/test-clickhouse-integration.sh`.
- data-api: drop `id` from both SELECT lists and `scanLogScore`, and select
  `toFloat64(offset)` (`chdb/logscores.go`)

Then scale the archiver back up. It resumes from
`log_scores_archive_status.log_score_id` and catches up on the backlog.

### NaN / Inf guard is mandatory

A survey of 11.6 billion non-null offsets found **zero** NaN or Inf values,
so this is forward-looking protection rather than a fix for existing data:
PostgreSQL `double precision` can store NaN where MySQL could not, and
`decimal.NewFromFloat` panics on it. The writer must reject or NULL such
values explicitly rather than passing them through.

## 5. Housekeeping

```sql
ALTER TABLE ntppool.log_scores_shard DROP PARTITION '2000-01-01';
ALTER TABLE ntpbeta.log_scores_shard DROP PARTITION '2000-01-01';
```

Drop the old local tables once the new ones have been serving for long
enough to be trusted. Drop the orphaned `ntpbeta.log_scores3` /
`log_scores_shard3` pair at the same time if not already done.
