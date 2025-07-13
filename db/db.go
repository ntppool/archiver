package db

import (
	"context"
	"fmt"
)

// Pool is the global connection pool
var Pool ConnectionPool

// Setup configures the database connection pool using file-based configuration
func Setup() error {
	pool, err := NewPool()
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
