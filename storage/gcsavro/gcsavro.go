package gcsavro

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	gstorage "cloud.google.com/go/storage"

	"go.ntppool.org/archiver/logscore"
	"go.ntppool.org/archiver/storage"
	"go.ntppool.org/archiver/storage/fileavro"
)

type gcsAvroArchiver struct {
	fileAvro   storage.FileArchiver
	bucketName string
	tempdir    string
}

// NewArchiver returns an archiver that stores data in avro files in the specified path
func NewArchiver() (storage.Archiver, error) {
	bucketName := os.Getenv("gc_bucket")
	if len(bucketName) == 0 {
		return nil, fmt.Errorf("gc_bucket must be set")
	}

	tempdir, err := os.MkdirTemp("", "gcsavro")
	if err != nil {
		return nil, err
	}

	fa, err := fileavro.NewArchiver(tempdir)
	if err != nil {
		return nil, err
	}

	a := &gcsAvroArchiver{
		fileAvro:   fa,
		bucketName: bucketName,
		tempdir:    tempdir,
	}

	return a, nil
}

func (a *gcsAvroArchiver) Close() error {
	os.RemoveAll(a.tempdir)
	return nil
}

func (a *gcsAvroArchiver) BatchSizeMinMaxTime() (int, int, time.Duration) {
	return a.fileAvro.BatchSizeMinMaxTime()
}

func (a *gcsAvroArchiver) Store(logscores []*logscore.LogScore) (int, error) {
	fh, err := os.CreateTemp("", "gcsavro-")
	if err != nil {
		return 0, err
	}

	// log.Printf("Temp FH: %s", fh.Name())

	n, err := a.fileAvro.StoreWriter(fh, logscores)
	if err != nil {
		return 0, err
	}
	_, err = fh.Seek(0, 0)
	if err != nil {
		return 0, err
	}

	fileName := a.fileAvro.(*fileavro.AvroArchiver).FileName(logscores)
	year := time.Unix(logscores[0].Ts, 0).UTC().Year()
	fileName = fmt.Sprintf("%d/%s", year, fileName)

	err = a.Upload(fh, fileName)
	if err != nil {
		return 0, err
	}

	err = fh.Close()

	os.Remove(fh.Name())

	return n, err
}

func (a *gcsAvroArchiver) Upload(fh io.ReadWriteCloser, path string) error {
	log.Printf("Uploading to %s/%s", a.bucketName, path)

	ctx := context.Background()
	client, err := gstorage.NewClient(ctx)
	if err != nil {
		return err
	}

	bucket := client.Bucket(a.bucketName).UserProject("ntppool")
	obj := bucket.Object(path)
	wc := obj.NewWriter(ctx)
	wc.ContentType = "avro/binary"
	wc.CacheControl = "public, max-age=157248000"

	if _, err = io.Copy(wc, fh); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}

	return nil
}
