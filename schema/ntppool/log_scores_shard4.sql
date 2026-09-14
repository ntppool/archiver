CREATE TABLE ntppool.log_scores_shard4
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
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toStartOfQuarter(ts)
ORDER BY (server_id, toStartOfWeek(ts), monitor_id, ts)
SETTINGS index_granularity = 8192
