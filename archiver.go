package archiver

import (
	"fmt"

	"github.com/ntppool/archiver/storage"
	"github.com/ntppool/archiver/storage/clickhouse"
	"github.com/ntppool/archiver/storage/fileavro"
	"github.com/ntppool/archiver/storage/gcsavro"
	"github.com/ntppool/archiver/storage/influxdb"
)

// SetupArchiver returns an Archiver type (mysql, influxdb, bigquery, ...)
func SetupArchiver(name string, config string) (storage.Archiver, error) {
	switch name {
	// case "mysql":
	case "influxdb":
		ia, err := influxdb.NewInfluxArchiver()
		if err != nil {
			return nil, err
		}
		return ia, nil
	case "fileavro":
		fa, err := fileavro.NewArchiver("avro-data")
		if err != nil {
			return nil, err
		}
		return fa, err
	case "gcsavro":
		return gcsavro.NewArchiver()
	case "clickhouse":
		return clickhouse.NewArchiver()
	// case "bigquery":
	// case "s3":
	// case "clickhouse":
	default:
		return nil, fmt.Errorf("unknown archiver '%s'", name)
	}
}
