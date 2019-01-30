package main

import (
	"log"

	"github.com/ntppool/archiver/db"
)

func main() {
	Execute()
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
