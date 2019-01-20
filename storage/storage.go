package storage

import "github.com/ntppool/archiver/logscore"

// Archiver is the interface definition for storing data points externally
type Archiver interface {
	BatchSizeMinMax() (int, int)
	Store(ls []*logscore.LogScore) (int, error)
	// Get(ServerID int) ([]LogScore, error)
}
