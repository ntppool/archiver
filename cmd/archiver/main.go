package main

import (
	"fmt"
	"log"
	"os"

	"github.com/ntppool/archiver/db"
	"github.com/ntppool/archiver/source"
	"github.com/ntppool/archiver/storage"
)

func main() {

	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", os.Getenv("db_user"), os.Getenv("db_pass"),
		os.Getenv("db_host"), os.Getenv("db_database"),
	)

	err := db.Setup(dsn)
	if err != nil {
		log.Fatalf("database connection: %s", err)
	}

	lock := getLock()
	if !lock {
		log.Printf("Did not get lock, exiting")
		os.Exit(2)
	}

	status, err := storage.GetArchiveStatus()
	if err != nil {
		log.Fatalf("archive status: %s", err)
	}

	source := source.New("log_scores_archive")

	for _, s := range status {
		err := source.Process(s)
		if err != nil {
			log.Printf("error processing %s: %s", s.Archiver, err)
		}

	}

}

func getLock() bool {
	// todo: replace with etcd leader
	var lock int
	err := db.DB.Get(&lock, `SELECT GET_LOCK("archiver", 0)`)
	if err != nil {
		log.Fatalf("lock: %s", err)
	}
	if lock == 1 {
		return true
	}
	return false
}
