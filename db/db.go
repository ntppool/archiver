package db

import (
	"fmt"
	"regexp"

	// import the mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"go.ntppool.org/archiver/config"
	"go.ntppool.org/common/logger"
)

// DB is the state database
var DB *sqlx.DB

// Setup the state database connection
func Setup(dsn string) error {
	log := logger.Setup()

	re := regexp.MustCompile(":.*?@")
	redacted := re.ReplaceAllString(dsn, ":...@")

	log.Debug("db connecting", "dsn", redacted)

	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return err
	}

	DB = db

	return nil
}

// SetupWithConfig configures the database connection using the provided config
func SetupWithConfig(cfg *config.Config) error {
	log := logger.Setup()

	dsn := cfg.GetMySQLDSN()
	re := regexp.MustCompile(":.*?@")
	redacted := re.ReplaceAllString(dsn, ":...@")

	log.Debug("db connecting", "dsn", redacted)

	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return err
	}

	// Configure connection pool using config values
	db.SetConnMaxIdleTime(cfg.Database.MaxIdleTime)
	db.SetConnMaxLifetime(cfg.Database.MaxLifetime)
	db.SetMaxIdleConns(cfg.Database.MaxIdleConns)
	db.SetMaxOpenConns(cfg.Database.MaxOpenConns)

	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ping: %s", err)
	}

	DB = db

	return nil
}
