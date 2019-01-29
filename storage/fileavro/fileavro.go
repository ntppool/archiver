package fileavro

import (
	"fmt"
	"log"
	"os"
	"path"

	"github.com/ntppool/archiver/logscore"
	"github.com/ntppool/archiver/storage"

	goavro "gopkg.in/linkedin/goavro.v2"
)

type avroArchiver struct {
	path string
}

const batchAppendSize = 50000

// NewArchiver returns an archiver that stores data in avro files in the specified path
func NewArchiver(path string) (storage.Archiver, error) {
	a := &avroArchiver{path: path}
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
func (a *avroArchiver) BatchSizeMinMax() (int, int) {
	return 1000, 1000000
}

// Store is for the Archiver interface
func (a *avroArchiver) Store(logscores []*logscore.LogScore) (int, error) {

	log.Println("Running Avro File batcher")

	codec, err := goavro.NewCodec(`
	{
	  "type": "record",
	  "name": "logscore",
	  "fields" : [
		  {"name": "id", "type": "long"},
		  {"name": "serverid", "type": "int"},
		  {"name": "monitorid", "type": "int"},
		  {"name": "ts", "type": "int"},
		  {"name": "score", "type": "float"},
		  {"name": "step", "type": "float"},
		  {"name": "offset", "type": ["null", "float"]},
		  {"name": "leap", "type": ["null", "int"]}
		 ]
	}`)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("Canonical Schema: %s\n", codec.CanonicalSchema())

	if len(logscores) == 0 {
		log.Printf("no input data!")
		return 0, nil
	}

	fileName := fmt.Sprintf("%d-%d.avro", logscores[0].Ts, logscores[0].ID)

	fileName = path.Join(a.path, fileName)

	fh, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return 0, fmt.Errorf("open file %q: %s", fileName, err)
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
			"id":        ls.ID,
			"serverid":  ls.ServerID,
			"monitorid": ls.MonitorID,
			"ts":        ls.Ts,
			"score":     ls.Score,
			"step":      ls.Step,
			"offset":    offset,
			"leap":      leap,
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

	err = fh.Close()
	if err != nil {
		return 0, err
	}
	return count, nil
}
