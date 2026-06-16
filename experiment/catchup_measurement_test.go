package experiment_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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
			if err != nil {
				t.Fatalf("NewCatchUpMsr() failed: %v", err)
			}
			defer cm.Close()

			tt.measurement(t, cm)

			cm.Flush()
			data, err := os.ReadFile(fn)
			if err != nil {
				t.Fatalf("failed to read measurement file: %v", err)
			}

			hasContent := len(data) > 0
			if hasContent != tt.expectEntry {
				t.Errorf("file has content = %v, expectEntry = %v", hasContent, tt.expectEntry)
			}
		})
	}
}
