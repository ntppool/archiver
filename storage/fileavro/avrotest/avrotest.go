package main

import (
	"fmt"
	"io/ioutil"
	"log"

	"go.ntppool.org/archiver/logscore"
	"go.ntppool.org/archiver/storage/fileavro"
)

func main() {
	tempdir, err := ioutil.TempDir("", "avro")
	if err != nil {
		log.Fatalf("tempdir: %s", err)
	}

	fmt.Printf("tempdir: %s", tempdir)
	// defer os.RemoveAll(tempdir)

	av, err := fileavro.NewArchiver(tempdir)
	if err != nil {
		log.Fatalf("could not NewArchiver(): %s", err)
	}

	ls := []*logscore.LogScore{}

	i, err := av.Store(ls)
	if err != nil {
		log.Fatalf("store(): %s", err)
	}
	log.Printf("i: %d", i)
}
