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
	// pg_try_advisory_lock returns true if lock acquired, false otherwise
	// We use hashtext() to convert the string name to an integer key
	var acquired bool
	err := db.Pool.Get(context.Background(), &acquired, `SELECT pg_try_advisory_lock(hashtext($1))`, name)
	if err != nil {
		log.Fatalf("lock: %s", err)
	}
	return acquired
}
