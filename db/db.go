package db

import (
	"fmt"
	"log"

	// import the mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// DB is the state database
var DB *sqlx.DB

// Setup the state database connection
func Setup(dsn string) error {
	log.Printf("connecting to %q", dsn)

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
