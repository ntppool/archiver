package archiver // import "go.ntppool.org/archiver"

import (
	"errors"
	"fmt"
	"os"

	"go.ntppool.org/archiver/storage"
	"go.ntppool.org/archiver/storage/bigquery"
	"go.ntppool.org/archiver/storage/cleanup"
	"go.ntppool.org/archiver/storage/clickhouse"
	"go.ntppool.org/archiver/storage/fileavro"
	"go.ntppool.org/archiver/storage/gcsavro"
)

// SetupArchiver returns an Archiver type (mysql, influxdb, bigquery, ...)
func SetupArchiver(name string, config string) (storage.Archiver, error) {
	switch name {
	// case "mysql":
	case "influxdb":
		return nil, errors.New("influxdb support has been removed")
	case "fileavro":
		avroPath := os.Getenv("avro_path")
		if len(avroPath) == 0 {
			return nil, fmt.Errorf("avro_path env not set for fileavro")
		}
		fa, err := fileavro.NewArchiver(avroPath)
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
