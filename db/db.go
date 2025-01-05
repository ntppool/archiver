package db

import (
	"fmt"
	"regexp"
	"time"

	// import the mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"go.ntppool.org/common/logger"
)

// DB is the state database
var DB *sqlx.DB

// Setup the state database connection
func Setup(dsn string) error {
	log := logger.Setup()

	dsn = dsn + "?&parseTime=true&charset=utf8mb4&rejectReadOnly=true&timeout=10s&loc=UTC"

	re := regexp.MustCompile(":.*?@")
	redacted := re.ReplaceAllString(dsn, ":...@")

	log.Debug("db connecting", "dsn", redacted)

	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return err
	}
	db.SetConnMaxIdleTime(2 * time.Minute)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)

	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ping: %s", err)
	}

	DB = db

	return nil
}
