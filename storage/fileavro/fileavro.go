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
	return 10, 500000
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
		  {"name": "leap", "type": "int"}
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

	for _, ls := range logscores {

		fmt.Printf("ls: %+v\n", ls)

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

		avromap := map[string]interface{}{
			"id":        ls.ID,
			"serverid":  ls.ServerID,
			"monitorid": ls.MonitorID,
			"ts":        ls.Ts,
			"score":     ls.Score,
			"step":      ls.Step,
			"offset":    offset,
			"leap":      ls.Meta.Leap,
		}

		// Convert native Go form to textual Avro data
		textual, err := codec.TextualFromNative(nil, avromap)
		if err != nil {
			return 0, fmt.Errorf("TextualFromNative: %s", err)
		}
		fmt.Printf("AVRO: %s\n", textual)

		// binary, err := codec.BinaryFromNative(nil, avromap)
		// if err != nil {
		// 	return 0, fmt.Errorf("BinaryFromNative: %s", err)
		// }

		queue = append(queue, avromap)

		if len(queue) > 500 {
			err = w.Append(queue)
			if err != nil {
				return 0, fmt.Errorf("Append: %s", err)
			}
			queue = []interface{}{}
		}
	}

	if len(queue) > 0 {
		err = w.Append(queue)
		if err != nil {
			return 0, fmt.Errorf("Append: %s", err)
		}
		queue = []interface{}{}
	}

	err = fh.Close()
	if err != nil {
		return 0, err
	}
	return 0, nil
}
