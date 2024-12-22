package cleanup

// this package implements the storage interface so the
// real "cleaner" (implemented inside the source package)
// can be scheduled like the real storage "drivers".

import (
	"fmt"
	"time"

	"go.ntppool.org/archiver/logscore"
	"go.ntppool.org/archiver/storage"
)

type fakeCleanup struct{}

// NewArchiver implements the storage interface
func NewArchiver() (storage.Archiver, error) {
	return &fakeCleanup{}, nil
}

// BatchSizeMinMaxTime implements the storage interface, it doesn't
// get used. The actual interval is in source/cleanup.go
func (a *fakeCleanup) BatchSizeMinMaxTime() (int, int, time.Duration) {
	return 0, 0, 10 * time.Minute
}

// Store implements the storage interface, but always returns an error
func (a *fakeCleanup) Store(ls []*logscore.LogScore) (int, error) {
	return 0, fmt.Errorf("cleanup can't store data")
}

// Close implements the storage interface, but doesn't do anything
func (a *fakeCleanup) Close() error {
	return nil
}
