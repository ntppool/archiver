package db

import (
	"context"
	"fmt"

	"go.ntppool.org/archiver/config"
)

// Pool is the global connection pool
var Pool ConnectionPool

// SetupWithConfig configures the database connection pool using the provided config
func SetupWithConfig(cfg *config.Config) error {
	pool, err := NewPool(cfg)
	if err != nil {
		return fmt.Errorf("creating connection pool: %w", err)
	}

	Pool = pool
	return nil
}

// GetPool returns the global connection pool
func GetPool() ConnectionPool {
	return Pool
}

// Ping verifies the database connection is alive
func Ping(ctx context.Context) error {
	if Pool == nil {
		return fmt.Errorf("database pool not initialized")
	}
	return Pool.Ping(ctx)
}
