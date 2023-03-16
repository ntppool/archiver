package logscore

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// LogScore is the data structure for the 'log_scores' table
// with monitoring measurements).
// Many of the fields have more precision here than in the database,
// this is kept to fit the avro / bigquery schema (int64 vs uint32).
type LogScore struct {
	ID        int64            `json:"id" msgpack:"id"`
	ServerID  int64            `json:"sid" msgpack:"sid"`
	MonitorID int64            `json:"mid" msgpack:"mid"`
	Ts        int64            `json:"ts" msgpack:"ts"`
	Score     float64          `json:"sc" msgpack:"sc"`
	Step      float64          `json:"st" msgpack:"st"`
	Offset    *float64         `json:"of" msgpack:"of"`
	RTT       *int64           `json:"rtt" msgpack:"rtt"`
	Meta      LogScoreMetadata `json:"attributes,omitempty"`
}

type LogScoreMetadata struct {
	Leap  uint8  `json:"leap,omitempty"`
	Error string `json:"error,omitempty"`
}

// JSON returns LogScore in JSON format plus a newline (\n) character
func (ls *LogScore) JSON() []byte {
	b, err := json.Marshal(ls)
	if err != nil {
		panic(err)
	}
	return append(b, '\n')
}

func (m *LogScoreMetadata) Value() (driver.Value, error) {
	return json.Marshal(m)
}

func (m *LogScoreMetadata) Scan(src interface{}) error {
	s, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("invalid value for token: %v", src)
	}
	return json.Unmarshal(s, &m)
}
