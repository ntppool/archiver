package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus"
	"go.ntppool.org/archiver/config"
	"go.ntppool.org/common/database"
	"go.ntppool.org/common/logger"
)

// ConnectionPool defines the interface for database operations with connection pooling
type ConnectionPool interface {
	Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	Begin(ctx context.Context) (*sql.Tx, error)
	Ping(ctx context.Context) error
	Close() error
	UpdateConfig(cfg *config.Config) error
}

// DatabasePool implements ConnectionPool using go.ntppool.org/common/database
type DatabasePool struct {
	mu     sync.RWMutex
	db     *sqlx.DB
	config *config.Config
	logger *slog.Logger
}

// NewPool creates a new connection pool
func NewPool(cfg *config.Config) (*DatabasePool, error) {
	p := &DatabasePool{
		config: cfg,
		logger: logger.Setup(),
	}

	if err := p.connect(); err != nil {
		return nil, fmt.Errorf("initial connection failed: %w", err)
	}

	return p, nil
}

// connect establishes the database connection using common/database
func (p *DatabasePool) connect() error {
	// Set DSN in environment variable for common database package
	dsn := p.config.GetMySQLDSN()
	os.Setenv("DATABASE_DSN", dsn)

	// Configure connection options with Prometheus metrics enabled
	options := database.ConfigOptions{
		ConfigFiles:          []string{}, // No config files, use env var
		EnablePoolMonitoring: true,
		PrometheusRegisterer: prometheus.DefaultRegisterer,
		MaxOpenConns:         p.config.Database.MaxOpenConns,
		MaxIdleConns:         p.config.Database.MaxIdleConns,
		ConnMaxLifetime:      p.config.Database.MaxLifetime,
	}

	// Open database connection using common package
	sqlDB, err := database.OpenDB(context.Background(), options)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}

	// Convert *sql.DB to *sqlx.DB
	p.db = sqlx.NewDb(sqlDB, "mysql")

	return nil
}

// Get executes a query that returns a single row
func (p *DatabasePool) Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	p.mu.RLock()
	db := p.db
	p.mu.RUnlock()

	return db.GetContext(ctx, dest, query, args...)
}

// Select executes a query that returns multiple rows
func (p *DatabasePool) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	p.mu.RLock()
	db := p.db
	p.mu.RUnlock()

	return db.SelectContext(ctx, dest, query, args...)
}

// Query executes a query and returns rows
func (p *DatabasePool) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	p.mu.RLock()
	db := p.db
	p.mu.RUnlock()

	return db.QueryContext(ctx, query, args...)
}

// Exec executes a query that doesn't return rows
func (p *DatabasePool) Exec(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	p.mu.RLock()
	db := p.db
	p.mu.RUnlock()

	return db.ExecContext(ctx, query, args...)
}

// Begin starts a transaction
func (p *DatabasePool) Begin(ctx context.Context) (*sql.Tx, error) {
	p.mu.RLock()
	db := p.db
	p.mu.RUnlock()

	return db.BeginTx(ctx, nil)
}

// Ping verifies the database connection is alive
func (p *DatabasePool) Ping(ctx context.Context) error {
	p.mu.RLock()
	db := p.db
	p.mu.RUnlock()

	return db.PingContext(ctx)
}

// Close closes the database connection
func (p *DatabasePool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// UpdateConfig dynamically updates the database configuration
func (p *DatabasePool) UpdateConfig(cfg *config.Config) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Close existing connection
	if p.db != nil {
		if err := p.db.Close(); err != nil {
			p.logger.Warn("error closing existing connection", "err", err)
		}
	}

	// Update config
	p.config = cfg

	// Reconnect with new configuration
	if err := p.connect(); err != nil {
		return fmt.Errorf("reconnecting with new config: %w", err)
	}

	p.logger.Info("database configuration updated successfully")
	return nil
}
