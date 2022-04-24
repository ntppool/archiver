package fileavro

import (
	"io/ioutil"
	"os"
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

	tempdir, err := ioutil.TempDir("", "fileavro")
	if err != nil || len(tempdir) == 0 {
		t.Fatalf("could not create temporary directory: %s", err)
	}
	defer os.RemoveAll(tempdir)

	av, err := NewArchiver(tempdir)
	if err != nil {
		t.Logf("could not NewArchiver(): %s", err)
		t.Fail()
	}

	rtt := int64(11234)

	ls := []*logscore.LogScore{
		{
			ID:        103535350,
			ServerID:  200,
			MonitorID: 1,
			Ts:        1547999353,
			Score:     19.2,
			Step:      0.9,
			Offset:    nil,
			RTT:       &rtt,
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
