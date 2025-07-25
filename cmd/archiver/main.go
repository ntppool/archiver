package main

import (
	"context"
	"log"

	"go.ntppool.org/archiver/db"
)

func main() {
	Execute()
}

func getLock(name string) bool {
	// todo: replace with etcd leader
	var lock int
	err := db.Pool.Get(context.Background(), &lock, `SELECT GET_LOCK(?, 0)`, name)
	if err != nil {
		log.Fatalf("lock: %s", err)
	}
	if lock == 1 {
		return true
	}
	return false
}
