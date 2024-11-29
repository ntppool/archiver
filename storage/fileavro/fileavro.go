package fileavro

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"time"

	"go.ntppool.org/archiver/logscore"
	"go.ntppool.org/archiver/storage"

	goavro "github.com/linkedin/goavro/v2"
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

// BatchSizeMinMaxTime returns the minimum and maximum batch size
func (a *AvroArchiver) BatchSizeMinMaxTime() (int, int, time.Duration) {
	return 500000, 10000000, time.Hour * 24
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

	fh, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0o666)
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
		  {"name": "rtt", "type": ["null", "int"]},
		  {"name": "leap", "type": ["null", "int"]},
		  {"name": "error", "type": ["null", "string"]}
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
		var rtt interface{}

		if ls.Offset == nil {
			offset = nil
		} else {
			offset = goavro.Union("float", *ls.Offset)
		}

		if ls.RTT == nil {
			rtt = nil
		} else {
			rtt = goavro.Union("int", *ls.RTT)
		}

		var leap interface{}
		if ls.Meta.Leap != 0 {
			leap = goavro.Union("int", int(ls.Meta.Leap))
		}

		var lsError interface{}
		if len(ls.Meta.Error) > 0 {
			lsError = goavro.Union("string", ls.Meta.Error)
		}

		avromap := map[string]interface{}{
			"id":         ls.ID,
			"server_id":  ls.ServerID,
			"monitor_id": ls.MonitorID,
			"ts":         time.Unix(ls.Ts, 0),
			"score":      ls.Score,
			"step":       ls.Step,
			"offset":     offset,
			"rtt":        rtt,
			"leap":       leap,
			"error":      lsError,
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
				return count, fmt.Errorf("append: %s", err)
			}
			count = count + len(queue)
			queue = []interface{}{}
		}
	}

	if len(queue) > 0 {
		err = w.Append(queue)
		if err != nil {
			return count, fmt.Errorf("append: %s", err)
		}
		count = count + len(queue)
		// queue = []interface{}{}
	}

	return count, nil
}

// Close finishes up the archiver
func (a *AvroArchiver) Close() error {
	return nil
}
