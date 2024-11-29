package bigquery

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"go.ntppool.org/archiver/logscore"
	"go.ntppool.org/archiver/storage"
	"go.ntppool.org/archiver/storage/fileavro"
)

type bqArchiver struct {
	fileAvro    storage.FileArchiver
	datasetName string
	tempdir     string
}

// NewArchiver returns an archiver that stores data in avro files in the specified path
func NewArchiver() (storage.Archiver, error) {
	datasetName := os.Getenv("bq_dataset")
	if len(datasetName) == 0 {
		return nil, fmt.Errorf("bq_dataset must be set")
	}

	tempdir, err := os.MkdirTemp("", "bqavro")
	if err != nil {
		return nil, err
	}

	fa, err := fileavro.NewArchiver(tempdir)
	if err != nil {
		return nil, err
	}

	a := &bqArchiver{
		fileAvro:    fa,
		datasetName: datasetName,
		tempdir:     tempdir,
	}

	return a, nil
}

func (a *bqArchiver) Close() error {
	os.RemoveAll(a.tempdir)
	return nil
}

func (a *bqArchiver) BatchSizeMinMaxTime() (int, int, time.Duration) {
	// we're limited to 1000 load jobs per table per day, so make
	// sure we stay way under by waiting 10 minutes between each
	return 200, 10000000, time.Minute * 10
}

func (a *bqArchiver) Store(logscores []*logscore.LogScore) (int, error) {
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

	err = a.Load(fh)
	if err != nil {
		return 0, err
	}

	err = fh.Close()

	os.Remove(fh.Name())

	return n, err
}

func (a *bqArchiver) Load(fh io.ReadWriteCloser) error {
	tableName := "log_scores"

	log.Printf("Loading into %s.%s", a.datasetName, tableName)

	r := bigquery.NewReaderSource(fh)
	r.SourceFormat = "AVRO"

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, "ntppool")
	if err != nil {
		return err
	}
	ds := client.Dataset(a.datasetName)
	table := ds.Table("log_scores")
	log.Printf("Table ID: %s", table.FullyQualifiedName())
	loader := table.LoaderFrom(r)
	job, err := loader.Run(ctx)
	if err != nil {
		return fmt.Errorf("could not run job: %s", err)
	}

	log.Printf("Loading BigQuery data with job %q", job.ID())
	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("error checking job status: %s", err)
	}
	if status.Err() != nil {
		return fmt.Errorf("job load error: %s", status.Err())
	}

	return nil
}
