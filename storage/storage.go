package storage

import (
	"io"

	"go.ntppool.org/archiver/logscore"
)

// Archiver is the interface definition for storing data points externally
type Archiver interface {
	BatchSizeMinMax() (int, int)
	Store(ls []*logscore.LogScore) (int, error)
	Close() error
	// Get(ServerID int) ([]LogScore, error)
}

// FileArchiver is like Archiver, but with an extra method to save data to a ReadWriter
type FileArchiver interface {
	Archiver
	StoreWriter(io.ReadWriter, []*logscore.LogScore) (int, error)
}
