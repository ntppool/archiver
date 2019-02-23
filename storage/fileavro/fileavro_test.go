package fileavro

import (
	"testing"

	"go.ntppool.org/archiver/logscore"
)

func TestStore(t *testing.T) {

	// tempdir, err := ioutil.TempDir("", "avro")
	// if err != nil {
	// 	t.Logf("tempdir: %s", err)
	// 	t.Fail()
	// }
	// t.Logf("tempdir: %s", tempdir)
	// defer os.RemoveAll(tempdir)

	tempdir := "/tmp/avro"

	av, err := NewArchiver(tempdir)
	if err != nil {
		t.Logf("could not NewArchiver(): %s", err)
		t.Fail()
	}

	ls := []*logscore.LogScore{
		&logscore.LogScore{
			ID:        103535350,
			ServerID:  200,
			MonitorID: 1,
			Ts:        1547999353,
			Score:     19.2,
			Step:      0.9,
			Offset:    nil,
			// &float64{0.212313413},
			Meta: logscore.LogScoreMetadata{Leap: 0},
		},
	}

	i, err := av.Store(ls)
	if err != nil {
		t.Logf("store(): %s", err)
		t.Fail()
	}
	t.Logf("i: %d", i)
}
