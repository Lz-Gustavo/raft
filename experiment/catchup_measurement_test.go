package experiment_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/raft/v3/experiment"
)

func TestCatchUpMsr_StartAndEnd(t *testing.T) {
	tests := []struct {
		name        string
		measurement func(*testing.T, *experiment.CatchUpMsr)
		expectedN   int
	}{
		{
			name: "start and end records duration",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start()
				time.Sleep(10 * time.Millisecond)
				cm.End()
			},
			expectedN: 1,
		},
		{
			name: "multiple measurements",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				for i := 0; i < 3; i++ {
					cm.Start()
					time.Sleep(5 * time.Millisecond)
					cm.End()
				}
			},
			expectedN: 3,
		},
		{
			name: "end without start does nothing",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.End() // Should not record anything
			},
			expectedN: 0,
		},
		{
			name: "start without end",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start()
				// Don't call End - should not panic
			},
			expectedN: 0,
		},
		{
			name: "multiple starts before end avoids overwriting measurement",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start()
				time.Sleep(10 * time.Millisecond)
				// Second Start() call should be ignored due to protection
				cm.Start()
				time.Sleep(5 * time.Millisecond)
				cm.End()
			},
			expectedN: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			fn := filepath.Join(tmpDir, "test-measurement.out")

			cm, err := experiment.NewCatchUpMsr(fn)
			assert.NoError(t, err, "NewCatchUpMsr() failed")
			defer cm.Close()

			tt.measurement(t, cm)

			cm.Flush()
			data, err := os.ReadFile(fn)
			assert.NoError(t, err, "failed to read measurement file")

			lines := bytes.Split(data, []byte("\n"))
			assert.Equal(t, tt.expectedN, len(lines)-1)
		})
	}
}
