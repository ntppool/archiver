package logscore

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

// LogScore is the data structure for the 'log_score' table (monitoring measurements)
type LogScore struct {
	ID        int64            `json:"id" msgpack:"id"`
	ServerID  uint32           `json:"sid" msgpack:"sid"`
	MonitorID uint32           `json:"mid" msgpack:"mid"`
	Ts        int64            `json:"ts" msgpack:"ts"`
	Score     float32          `json:"sc" msgpack:"sc"`
	Step      float32          `json:"st" msgpack:"st"`
	Offset    *float64         `json:"of" msgpack:"of"`
	RTT       *uint32          `json:"rtt" msgpack:"rtt"`
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
