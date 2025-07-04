package archiver // import "go.ntppool.org/archiver"

import (
	"errors"
	"fmt"

	"go.ntppool.org/archiver/config"
	"go.ntppool.org/archiver/storage"
	"go.ntppool.org/archiver/storage/bigquery"
	"go.ntppool.org/archiver/storage/cleanup"
	"go.ntppool.org/archiver/storage/clickhouse"
	"go.ntppool.org/archiver/storage/fileavro"
	"go.ntppool.org/archiver/storage/gcsavro"
)

// SetupArchiver returns an Archiver type (mysql, influxdb, bigquery, ...)
func SetupArchiver(name string, configParam string) (storage.Archiver, error) {
	switch name {
	// case "mysql":
	case "influxdb":
		return nil, errors.New("influxdb support has been removed")
	case "fileavro":
		// Load config to get avro_path
		cfg, err := config.LoadGlobalConfig()
		if err != nil {
			return nil, fmt.Errorf("loading config: %w", err)
		}
		if len(cfg.Storage.AvroPath) == 0 {
			return nil, fmt.Errorf("avro_path not set for fileavro")
		}
		fa, err := fileavro.NewArchiver(cfg.Storage.AvroPath)
		if err != nil {
			return nil, err
		}
		return fa, err
	case "gcsavro":
		return gcsavro.NewArchiver()
	case "bigquery":
		return bigquery.NewArchiver()

	case "clickhouse":
		return clickhouse.NewArchiver()

	case "cleanup":
		return cleanup.NewArchiver()

	// case "bigquery":
	// case "s3":
	// case "clickhouse":
	default:
		return nil, fmt.Errorf("unknown archiver '%s'", name)
	}
}
