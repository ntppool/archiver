package archiver

import (
	"fmt"
	"log"
	"math"
	"net/url"
	"os"
	"strconv"
	"time"

	influx "github.com/influxdata/influxdb/client"
)

// InfluxArchiver stores log scores to InfluxDB
type InfluxArchiver struct {
	conn *influx.Client
}

// NewInfluxArchiver returns a InfluxArchiver (including testing the connection)
func NewInfluxArchiver() (*InfluxArchiver, error) {
	a := &InfluxArchiver{}
	conn, err := a.influxConn()
	if err != nil {
		return nil, err
	}
	a.conn = conn
	return a, nil
}

// BatchSizeMinMax returns the minimum and maximum batch size for InfluxArchiver
func (a *InfluxArchiver) BatchSizeMinMax() (int, int) {
	return 100, 50000
}

func (a *InfluxArchiver) influxConn() (*influx.Client, error) {
	// u, err := url.Parse(fmt.Sprintf("https://%s:%d", "influxdb.ntppool.net", 8086))
	u, err := url.Parse(fmt.Sprintf("http://%s:%d", "localhost", 8086))
	if err != nil {
		log.Fatal(err)
	}

	conf := influx.Config{
		URL:      *u,
		Username: os.Getenv("INFLUX_USER"),
		Password: os.Getenv("INFLUX_PASSWORD"),
	}
	con, err := influx.NewClient(conf)
	if err != nil {
		return nil, err
	}

	dur, ver, err := con.Ping()
	if err != nil {
		return nil, err
	}
	log.Printf("Happy as a Hippo! %v, %s", dur, ver)

	return con, nil
}

// Store is for the Archiver interface
func (a *InfluxArchiver) Store(logscores []*LogScore) (int, error) {

	log.Println("Running Influx batcher")

	batch := influx.BatchPoints{
		Database: "ntpbeta",
		// Tags:     map[string]string{"log": "geodns"},
	}

	done := false

	delay := time.Second * 0

	for _, ls := range logscores {

		if done {
			break
		}

		// fmt.Print("-")
		point := influx.Point{}
		point.Tags = map[string]string{
			"Server":  strconv.FormatInt(ls.ServerID, 10),
			"Monitor": strconv.FormatInt(ls.MonitorID, 10),
		}
		point.Measurement = "logscore"

		point.Fields = map[string]interface{}{
			"ID":    ls.ID,
			"Score": ls.Score,
			"Step":  ls.Step,
		}
		if ls.Offset != nil {
			point.Fields["Offset"] = *ls.Offset
			point.Fields["OffsetAbs"] = math.Abs(*ls.Offset)
		}

		point.Time = time.Unix(ls.Ts, 0)
		batch.Points = append(batch.Points, point)
		if len(batch.Points) > 15000 {
			if delay > 0 {
				log.Printf("Sleeping %d seconds", delay)
				time.Sleep(time.Second * delay)
			}
			resp, err := a.conn.Write(batch)
			if err != nil {
				log.Printf("Error writing influx batch: %s, '%s' (%t)", err, resp.Error(), resp.Error())
				delay = (4 + (delay * delay)) / 2
				if delay > 300 {
					delay = 300
				}
			} else {
				if delay > 0 {
					delay = delay / 2
				}
				batch.Points = make([]influx.Point, 0)
			}

		}
	}

	if len(batch.Points) > 0 {
		_, err := a.conn.Write(batch)
		if err != nil {
			log.Printf("Error writing influx batch: %s", err)
			return 0, err
		}
	}

	return 0, nil
}
