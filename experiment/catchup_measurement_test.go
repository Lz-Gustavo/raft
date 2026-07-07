package experiment_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/raft/v3/experiment"
	"go.etcd.io/raft/v3/tracker"
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

func TestCatchUpMsr_StartAndEndDebug(t *testing.T) {
	tests := []struct {
		name        string
		measurement func(*testing.T, *experiment.CatchUpMsr)
		expectedN   int
		assertions  []string
	}{
		{
			name: "start and end debug records tagged payload",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start()
				time.Sleep(10 * time.Millisecond)
				cm.EndDebug(experiment.CatchUpDebugInfo{
					LogEntries:        9,
					LeaderFirstIndex:  10,
					LeaderLastIndex:   18,
					LeaderCommitted:   17,
					LeaderApplied:     16,
					FollowerMatch:     18,
					FollowerNext:      19,
					FollowerInflights: 0,
					FollowerState:     tracker.StateReplicate,
				})
			},
			expectedN: 1,
			assertions: []string{
				"[",
				"]",
				"logEntries:9",
				"leader:{firstIndex:10, lastIndex:18, committed:17, applied:16}",
				"follower:{match:18, next:19, inflights:0, state:StateReplicate}",
			},
		},
		{
			name: "multiple debug measurements",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				for i := 0; i < 3; i++ {
					cm.Start()
					time.Sleep(5 * time.Millisecond)
					cm.EndDebug(experiment.CatchUpDebugInfo{
						LogEntries:        1,
						LeaderFirstIndex:  1,
						LeaderLastIndex:   1,
						LeaderCommitted:   1,
						LeaderApplied:     1,
						FollowerMatch:     1,
						FollowerNext:      2,
						FollowerInflights: 0,
						FollowerState:     tracker.StateReplicate,
					})
				}
			},
			expectedN: 3,
		},
		{
			name: "end without start does nothing",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.EndDebug(experiment.CatchUpDebugInfo{})
			},
			expectedN: 0,
		},
		{
			name: "start without end",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start()
				// Don't call EndDebug - should not panic
			},
			expectedN: 0,
		},
		{
			name: "multiple starts before debug end avoids overwriting measurement",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start()
				time.Sleep(10 * time.Millisecond)
				// Second Start() call should be ignored due to protection
				cm.Start()
				time.Sleep(5 * time.Millisecond)
				cm.EndDebug(experiment.CatchUpDebugInfo{
					LogEntries:        1,
					LeaderFirstIndex:  1,
					LeaderLastIndex:   1,
					LeaderCommitted:   1,
					LeaderApplied:     1,
					FollowerMatch:     1,
					FollowerNext:      2,
					FollowerInflights: 0,
					FollowerState:     tracker.StateReplicate,
				})
			},
			expectedN: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			fn := filepath.Join(tmpDir, "test-measurement-debug.out")

			cm, err := experiment.NewCatchUpMsr(fn)
			assert.NoError(t, err, "NewCatchUpMsr() failed")
			defer cm.Close()

			tt.measurement(t, cm)

			cm.Flush()
			data, err := os.ReadFile(fn)
			assert.NoError(t, err, "failed to read measurement file")

			lines := bytes.Split(data, []byte("\n"))
			assert.Equal(t, tt.expectedN, len(lines)-1)

			if tt.expectedN > 0 {
				got := string(lines[0])
				for _, want := range tt.assertions {
					assert.Contains(t, got, want)
				}
			}
		})
	}
}
