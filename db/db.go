package db

import (
	"fmt"
	"log"
	"regexp"

	// import the mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// DB is the state database
var DB *sqlx.DB

// Setup the state database connection
func Setup(dsn string) error {

	dsn = dsn + "?&parseTime=true&loc=UTC"

	re := regexp.MustCompile(":.*?@")
	redacted := re.ReplaceAllString(dsn, ":...@")

	log.Printf("connecting to %q", redacted)

	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		return err
	}

	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ping: %s", err)
	}

	DB = db

	return nil
}
