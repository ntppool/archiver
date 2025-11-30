package db

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.ntppool.org/common/database/pgdb"
	"go.ntppool.org/common/logger"
)

// ConnectionPool defines the interface for database operations with connection pooling
type ConnectionPool interface {
	Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error)
	Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
	Ping(ctx context.Context) error
	Close() error
	UpdateConfig() error
}

// DatabasePool implements ConnectionPool using pgxpool
type DatabasePool struct {
	mu     sync.RWMutex
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewPool creates a new connection pool using common database package configuration
func NewPool() (*DatabasePool, error) {
	p := &DatabasePool{
		logger: logger.Setup(),
	}

	if err := p.connect(); err != nil {
		return nil, fmt.Errorf("initial connection failed: %w", err)
	}

	return p, nil
}

// connect establishes the database connection using common/database/pgdb
func (p *DatabasePool) connect() error {
	ctx := context.Background()

	// Use default pool options which read from database.yaml or /vault/secrets/database.yaml
	options := pgdb.DefaultPoolOptions()
	options.MaxConns = 25
	options.MinConns = 2

	pool, err := pgdb.OpenPool(ctx, options)
	if err != nil {
		return fmt.Errorf("opening database pool: %w", err)
	}

	p.pool = pool
	return nil
}

// Get executes a query that returns a single row and scans it into dest
func (p *DatabasePool) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	p.mu.RLock()
	pool := p.pool
	p.mu.RUnlock()

	return pgxscan.Get(ctx, pool, dest, query, args...)
}

// Select executes a query that returns multiple rows and scans them into dest
func (p *DatabasePool) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	p.mu.RLock()
	pool := p.pool
	p.mu.RUnlock()

	return pgxscan.Select(ctx, pool, dest, query, args...)
}

// Query executes a query and returns rows
func (p *DatabasePool) Query(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	p.mu.RLock()
	pool := p.pool
	p.mu.RUnlock()

	return pool.Query(ctx, query, args...)
}

// Exec executes a query that doesn't return rows
func (p *DatabasePool) Exec(ctx context.Context, query string, args ...interface{}) (pgconn.CommandTag, error) {
	p.mu.RLock()
	pool := p.pool
	p.mu.RUnlock()

	return pool.Exec(ctx, query, args...)
}

// Begin starts a transaction
func (p *DatabasePool) Begin(ctx context.Context) (pgx.Tx, error) {
	p.mu.RLock()
	pool := p.pool
	p.mu.RUnlock()

	return pool.Begin(ctx)
}

// Ping verifies the database connection is alive
func (p *DatabasePool) Ping(ctx context.Context) error {
	p.mu.RLock()
	pool := p.pool
	p.mu.RUnlock()

	return pool.Ping(ctx)
}

// Close closes the database connection pool
func (p *DatabasePool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pool != nil {
		p.pool.Close()
	}
	return nil
}

// UpdateConfig dynamically updates the database configuration by reconnecting
func (p *DatabasePool) UpdateConfig() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Close existing connection
	if p.pool != nil {
		p.pool.Close()
	}

	// Reconnect with new configuration (re-reads config file)
	if err := p.connect(); err != nil {
		return fmt.Errorf("reconnecting with new config: %w", err)
	}

	p.logger.Info("database configuration updated successfully")
	return nil
}
