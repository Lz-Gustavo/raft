package experiment_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/raft/v3/experiment"
)

const (
	follower2 = uint64(2)
	follower3 = uint64(3)
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
				cm.Start(follower2)
				time.Sleep(10 * time.Millisecond)
				cm.End(follower2)
			},
			expectedN: 1,
		},
		{
			name: "multiple measurements",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				for i := 0; i < 3; i++ {
					cm.Start(follower2)
					time.Sleep(5 * time.Millisecond)
					cm.End(follower2)
				}
			},
			expectedN: 3,
		},
		{
			name: "end without start does nothing",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.End(follower2) // Should not record anything
			},
			expectedN: 0,
		},
		{
			name: "start without end",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2)
				// Don't call End - should not panic
			},
			expectedN: 0,
		},
		{
			name: "multiple starts before end avoids overwriting measurement",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2)
				time.Sleep(10 * time.Millisecond)
				// Second Start() call should be ignored due to protection
				cm.Start(follower2)
				time.Sleep(5 * time.Millisecond)
				cm.End(follower2)
			},
			expectedN: 1,
		},
		{
			name: "two followers tracked independently do not cross-contaminate",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2)
				time.Sleep(5 * time.Millisecond)
				cm.Start(follower3)
				time.Sleep(5 * time.Millisecond)
				cm.End(follower2)
				cm.End(follower3)
			},
			expectedN: 2,
		},
		{
			name: "cancel discards measurement without recording",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2)
				time.Sleep(5 * time.Millisecond)
				cm.Cancel(follower2)
				cm.End(follower2)
			},
			expectedN: 0,
		},
		{
			name: "start after cancel is not blocked by the discarded entry",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2)
				time.Sleep(5 * time.Millisecond)
				cm.Cancel(follower2)

				cm.Start(follower2)
				time.Sleep(5 * time.Millisecond)
				cm.End(follower2)
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

func TestCatchUpMsr_IsActiveAndActiveFollowers(t *testing.T) {
	tmpDir := t.TempDir()
	fn := filepath.Join(tmpDir, "test-active.out")

	cm, err := experiment.NewCatchUpMsr(fn)
	assert.NoError(t, err, "NewCatchUpMsr() failed")
	defer cm.Close()

	assert.False(t, cm.IsActive(follower2))
	assert.Empty(t, cm.ActiveFollowers())

	cm.Start(follower2)
	assert.True(t, cm.IsActive(follower2))
	assert.ElementsMatch(t, []uint64{follower2}, cm.ActiveFollowers())

	cm.Start(follower3)
	assert.ElementsMatch(t, []uint64{follower2, follower3}, cm.ActiveFollowers())

	cm.Cancel(follower2)
	assert.False(t, cm.IsActive(follower2))
	assert.ElementsMatch(t, []uint64{follower3}, cm.ActiveFollowers())

	cm.End(follower3)
	assert.False(t, cm.IsActive(follower3))
	assert.Empty(t, cm.ActiveFollowers())
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
				cm.Start(follower2)
				time.Sleep(10 * time.Millisecond)
				cm.EndDebug(follower2, experiment.CatchUpDebugInfo{
					FollowerID:       follower2,
					LogEntries:       9,
					LeaderFirstIndex: 10,
					LeaderLastIndex:  18,
					LeaderCommitted:  17,
					LeaderApplied:    16,
					FollowerMatch:    18,
					FollowerNext:     19,
				})
			},
			expectedN: 1,
			assertions: []string{
				"[",
				"]",
				"logEntries:9",
				"logleader:{firstIndex:10, lastIndex:18, committed:17, applied:16}",
				"follower:{id:2, match:18, next:19}",
			},
		},
		{
			name: "multiple debug measurements",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				for range 3 {
					cm.Start(follower2)
					time.Sleep(5 * time.Millisecond)
					cm.EndDebug(follower2, experiment.CatchUpDebugInfo{
						FollowerID:       follower2,
						LogEntries:       1,
						LeaderFirstIndex: 1,
						LeaderLastIndex:  1,
						LeaderCommitted:  1,
						LeaderApplied:    1,
						FollowerMatch:    1,
						FollowerNext:     2,
					})
				}
			},
			expectedN: 3,
		},
		{
			name: "end without start does nothing",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.EndDebug(follower2, experiment.CatchUpDebugInfo{})
			},
			expectedN: 0,
		},
		{
			name: "start without end",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2)
				// Don't call EndDebug - should not panic
			},
			expectedN: 0,
		},
		{
			name: "multiple starts before debug end avoids overwriting measurement",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2)
				time.Sleep(10 * time.Millisecond)
				// Second Start() call should be ignored due to protection
				cm.Start(follower2)
				time.Sleep(5 * time.Millisecond)
				cm.EndDebug(follower2, experiment.CatchUpDebugInfo{
					FollowerID:       follower2,
					LeaderFirstIndex: 1,
					LeaderLastIndex:  1,
					LeaderCommitted:  1,
					LeaderApplied:    1,
					FollowerMatch:    1,
					FollowerNext:     2,
				})
			},
			expectedN: 1,
		},
		{
			name: "two followers tracked independently in debug mode",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2)
				cm.Start(follower3)
				cm.EndDebug(follower2, experiment.CatchUpDebugInfo{FollowerID: follower2})
				cm.EndDebug(follower3, experiment.CatchUpDebugInfo{FollowerID: follower3})
			},
			expectedN: 2,
			assertions: []string{
				"follower:{id:2",
			},
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
				assert.True(t, strings.HasPrefix(got, "2:"), "expected line to be keyed by followerID 2, got: %s", got)
				for _, want := range tt.assertions {
					assert.Contains(t, got, want)
				}
			}
		})
	}
}
