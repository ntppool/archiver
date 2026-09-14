-- Post-migration definition: routes to log_scores_shard4.
-- Applying this file IS the cut-over step; see ../ntppool/log_scores_shard4_deploy.md.
CREATE TABLE askntp.log_scores
(
    `monitor_id` UInt32 CODEC(T64, ZSTD(3)),
    `server_id` UInt32 CODEC(DoubleDelta, ZSTD(3)),
    `ts` DateTime CODEC(DoubleDelta, ZSTD(3)),
    `score` Float32 CODEC(Gorilla, ZSTD(3)),
    `step` Float32 CODEC(ZSTD(3)),
    `offset` Nullable(Decimal64(7)) CODEC(T64, ZSTD(3)),
    `rtt` Nullable(UInt32) CODEC(T64, ZSTD(3)),
    `leap` Nullable(UInt8) CODEC(ZSTD(3)),
    `warning` LowCardinality(Nullable(String)) CODEC(ZSTD(3)),
    `error` LowCardinality(Nullable(String)) CODEC(ZSTD(3)),
    `dt` Date ALIAS toDate(ts)
)
ENGINE = Distributed('scores', 'askntp', 'log_scores_shard4', server_id)
