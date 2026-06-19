package experiment_test

import (
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
		expectEntry bool
	}{
		{
			name: "start and end records duration",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start()
				time.Sleep(10 * time.Millisecond)
				cm.End()
			},
			expectEntry: true,
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
			expectEntry: true,
		},
		{
			name: "end without start does nothing",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.End() // Should not record anything
			},
			expectEntry: false,
		},
		{
			name: "start without end",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start()
				// Don't call End - should not panic
			},
			expectEntry: false,
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

			hasContent := len(data) > 0
			assert.Equal(t, hasContent, tt.expectEntry)
		})
	}
}
