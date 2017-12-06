package target

import "encoding/json"

type LogScore struct{}

type Target interface {
	Name() string
	Save([]*LogScore) error
}

type Targets map[string]*Target

// ParseConfig parses a json.RawMessage for target configurations
func ParseConfig(js *json.RawMessage) (Targets, error) {
	return nil, nil
}
