package fileavro

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"time"

	"github.com/ntppool/archiver/logscore"
	"github.com/ntppool/archiver/storage"

	goavro "gopkg.in/linkedin/goavro.v2"
)

// AvroArchiver stores avro files to a file system path
type AvroArchiver struct {
	path string
}

const batchAppendSize = 50000

// NewArchiver returns an archiver that stores data in avro files in the specified path
func NewArchiver(path string) (storage.FileArchiver, error) {
	a := &AvroArchiver{path: path}
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("path %q is not a directory", path)
	}
	return a, nil
}

// BatchSizeMinMax returns the minimum and maximum batch size for InfluxArchiver
func (a *AvroArchiver) BatchSizeMinMax() (int, int) {
	return 500000, 10000000
}

// FileName returns the suggested filename for the given logscores
func (a *AvroArchiver) FileName(logscores []*logscore.LogScore) string {
	if len(logscores) == 0 {
		return ""
	}
	return fmt.Sprintf("%d-%d.avro", logscores[0].Ts, logscores[0].ID)
}

// Store is for the Archiver interface
func (a *AvroArchiver) Store(logscores []*logscore.LogScore) (int, error) {

	if len(logscores) == 0 {
		log.Printf("no input data!")
		return 0, nil
	}

	fileName := a.FileName(logscores)
	fileName = path.Join(a.path, fileName)

	fh, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return 0, fmt.Errorf("open file %q: %s", fileName, err)
	}

	n, err := a.StoreWriter(fh, logscores)

	if err != nil {
		os.Remove(fileName)
		return 0, err
	}

	err = fh.Close()
	if err != nil {
		return 0, err
	}

	return n, err
}

// StoreWriter is like store, but writes to the specified ReadWriter
func (a *AvroArchiver) StoreWriter(fh io.ReadWriter, logscores []*logscore.LogScore) (int, error) {

	log.Println("Running Avro File batcher")

	codec, err := goavro.NewCodec(`
	{
	  "type": "record",
	  "name": "logscore",
	  "fields" : [
		  {"name": "id", "type": "long"},
		  {"name": "server_id", "type": "int"},
		  {"name": "monitor_id", "type": "int"},
		  {"name": "ts", "type": "long", "logicalType": "timestamp-micros"},
		  {"name": "score", "type": "float"},
		  {"name": "step", "type": "float"},
		  {"name": "offset", "type": ["null", "float"]},
		  {"name": "leap", "type": ["null", "int"]}
		 ]
	}`)
	if err != nil {
		return 0, err
	}

	// fmt.Printf("Canonical Schema: %s\n", codec.CanonicalSchema())

	if len(logscores) == 0 {
		log.Printf("no input data!")
		return 0, nil
	}

	ocfconfig := goavro.OCFConfig{
		W:               fh,
		Codec:           codec,
		CompressionName: "null",
	}

	w, err := goavro.NewOCFWriter(ocfconfig)
	if err != nil {
		return 0, fmt.Errorf("NewOCFWriter: %s", err)
	}

	queue := []interface{}{}
	count := 0

	for _, ls := range logscores {

		// fmt.Printf("ls: %+v\n", ls)

		// // Convert native Go form to binary Avro data
		// binary, err := codec.BinaryFromNative(nil, native)
		// if err != nil {
		// 	fmt.Println(err)
		// }

		var offset interface{}

		if ls.Offset == nil {
			offset = nil
		} else {
			offset = goavro.Union("float", *ls.Offset)
		}

		var leap interface{}
		if ls.Meta.Leap != 0 {
			leap = goavro.Union("int", int(ls.Meta.Leap))
		}

		avromap := map[string]interface{}{
			"id":         ls.ID,
			"server_id":  ls.ServerID,
			"monitor_id": ls.MonitorID,
			"ts":         time.Unix(ls.Ts, 0),
			"score":      ls.Score,
			"step":       ls.Step,
			"offset":     offset,
			"leap":       leap,
		}

		// textual, err := codec.TextualFromNative(nil, avromap)
		// if err != nil {
		// 	return 0, fmt.Errorf("TextualFromNative: %s", err)
		// }
		// fmt.Printf("AVRO: %s\n", textual)

		queue = append(queue, avromap)

		if len(queue) > batchAppendSize {
			err = w.Append(queue)
			if err != nil {
				return count, fmt.Errorf("Append: %s", err)
			}
			count = count + len(queue)
			queue = []interface{}{}
		}
	}

	if len(queue) > 0 {
		err = w.Append(queue)
		if err != nil {
			return count, fmt.Errorf("Append: %s", err)
		}
		count = count + len(queue)
		queue = []interface{}{}
	}

	return count, nil
}
