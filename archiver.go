package archiver

import "fmt"

// Archiver is the interface definition for storing data points externally
type Archiver interface {
	BatchSizeMinMax() (int, int)
	Store(ls []*LogScore) (int, error)
	// Get(ServerID int) ([]LogScore, error)
}

// SetupArchiver returns an Archiver type (mysql, influxdb, bigquery, ...)
func SetupArchiver(name string, config string) (Archiver, error) {
	switch name {
	// case "mysql":
	case "influxdb":
		ia, err := NewInfluxArchiver()
		if err != nil {
			return nil, err
		}
		return ia, nil
	// case "bigquery":
	default:
		return nil, fmt.Errorf("unknown archiver '%s'", name)
	}
}
