package main

import (
	"context"
	"fmt"
	"log"

	"go.ntppool.org/archiver/config"
	"go.ntppool.org/archiver/db"
	"go.ntppool.org/archiver/source"
	"go.ntppool.org/archiver/storage"
)

func runArchive(table string, cfg *config.Config) error {
	ctx := context.Background()
	// Validate table name
	if !cfg.IsValidTable(table) {
		return fmt.Errorf("invalid table name '%s', must be one of: %v", table, cfg.App.ValidTables)
	}

	err := db.SetupWithConfig(cfg)
	if err != nil {
		return fmt.Errorf("database connection: %s", err)
	}

	if err = db.Ping(ctx); err != nil {
		log.Fatalf("Could not connect to database: %s", err)
	}

	// todo: make this be a goroutine that waits for a signal to release the lock
	lock := getLock(cfg.GetLockName(cfg.Database.Database))
	if !lock {
		return fmt.Errorf("did not get lock, exiting")
	}

	status, err := storage.GetArchiveStatus(ctx)
	if err != nil {
		return fmt.Errorf("archive status: %s", err)
	}

	source, err := source.New(table, cfg.App.RetentionDays)
	if err != nil {
		return fmt.Errorf("error creating source: %s", err)
	}

	for _, s := range status {

		if s.Archiver == "cleanup" {
			err = source.Cleanup(ctx, s)
			if err != nil {
				log.Printf("error running cleanup: %s", err)
			}
			continue
		}

		err := source.Process(ctx, s)
		if err != nil {
			return fmt.Errorf("error processing %s: %s", s.Archiver, err)
		}

	}

	return nil
}
