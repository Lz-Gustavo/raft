package experiment_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/raft/v3/experiment"
)

func TestThrMsr_Measurement(t *testing.T) {
	tmpDir := t.TempDir()
	fn := filepath.Join(tmpDir, "test-throughput.out")

	tm, err := experiment.NewThrMsr(fn)
	assert.NoError(t, err, "NewThrMsr() failed")
	defer tm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go tm.Run(ctx)

	numCounts := 5
	for i := 0; i < numCounts; i++ {
		tm.Count()
		time.Sleep(10 * time.Millisecond)
	}

	// allow some time for the ticker to record the throughput
	time.Sleep(2 * time.Second)

	cancel()
	tm.Flush()

	data, err := os.ReadFile(fn)
	assert.NoError(t, err, "failed to read measurement file")
	assert.NotEmpty(t, data)

	// check for total measurements = issued counts
	lines := bytes.Split(data, []byte("\n"))
	n := 0

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		thr, err := strconv.Atoi(string(line))
		assert.NoError(t, err, "failed to convert int measurement from str")
		n += thr
	}
	assert.Equal(t, numCounts, n)
}
