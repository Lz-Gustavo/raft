package experiment_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/experiment"
)

const (
	follower2 = uint64(2)
	follower3 = uint64(3)

	testSilence = 50 * time.Millisecond
)

// newTestMsr returns a measurer writing to a temporary file, along with a reader closing
// over that file that flushes and splits the recorded lines.
func newTestMsr(t *testing.T) (*experiment.CatchUpMsr, func() []string) {
	t.Helper()

	fn := filepath.Join(t.TempDir(), "test-measurement.out")
	cm, err := experiment.NewCatchUpMsr(fn, testSilence)
	require.NoError(t, err, "NewCatchUpMsr() failed")
	t.Cleanup(cm.Close)

	return cm, func() []string {
		t.Helper()

		cm.Flush()
		data, err := os.ReadFile(fn)
		require.NoError(t, err, "failed to read measurement file")

		if len(data) == 0 {
			return nil
		}
		return strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	}
}

// parseLine breaks a "<phase>:<followerID>:<startNs>:<durationNs>" record apart, ignoring
// any trailing debug payload.
func parseLine(t *testing.T, line string) (phase string, id uint64, startNs, durNs int64) {
	t.Helper()

	fields := strings.SplitN(strings.SplitN(line, " ", 2)[0], ":", 4)
	require.Len(t, fields, 4, "malformed measurement line: %s", line)

	id, err := strconv.ParseUint(fields[1], 10, 64)
	require.NoError(t, err)
	startNs, err = strconv.ParseInt(fields[2], 10, 64)
	require.NoError(t, err)
	durNs, err = strconv.ParseInt(fields[3], 10, 64)
	require.NoError(t, err)

	return fields[0], id, startNs, durNs
}

// promotedStart opens an episode for id and clears the peer-silence gate for it, standing
// in for what stepLeader does once another voter went quiet.
func promotedStart(cm *experiment.CatchUpMsr, id uint64, targetIndex uint64) {
	peerLastResp := time.Now().UnixNano()
	cm.Start(id, targetIndex)
	cm.Promote(id, peerLastResp)
}

func TestCatchUpMsr_Episodes(t *testing.T) {
	tests := []struct {
		name        string
		measurement func(*testing.T, *experiment.CatchUpMsr)
		expectedN   int
		assertions  func(*testing.T, []string)
	}{
		{
			name: "episode that never cleared the peer silence gate records nothing",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100)
				time.Sleep(5 * time.Millisecond)
				cm.EndRecovery(follower2)
				cm.EndReplication(follower2)
			},
			expectedN: 0,
		},
		{
			name: "promoted episode records both phases from a single start",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				promotedStart(cm, follower2, 100)
				time.Sleep(5 * time.Millisecond)
				cm.EndRecovery(follower2)
				time.Sleep(5 * time.Millisecond)
				cm.EndReplication(follower2)
			},
			expectedN: 2,
			assertions: func(t *testing.T, lines []string) {
				recPhase, recID, recStart, recDur := parseLine(t, lines[0])
				repPhase, repID, repStart, repDur := parseLine(t, lines[1])

				assert.Equal(t, "recovery", recPhase)
				assert.Equal(t, "replication", repPhase)
				assert.Equal(t, follower2, recID)
				assert.Equal(t, follower2, repID)
				assert.Equal(t, recStart, repStart, "both phases must share the episode start")
				assert.Greater(t, repDur, recDur, "replication is cumulative, so it contains recovery")
			},
		},
		{
			name: "single shot, nothing is recorded after a complete episode",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				promotedStart(cm, follower2, 100)
				cm.EndRecovery(follower2)
				cm.EndReplication(follower2)

				promotedStart(cm, follower3, 200)
				cm.EndRecovery(follower3)
				cm.EndReplication(follower3)
			},
			expectedN: 2,
			assertions: func(t *testing.T, lines []string) {
				for _, line := range lines {
					_, id, _, _ := parseLine(t, line)
					assert.Equal(t, follower2, id, "the second episode must not be recorded")
				}
			},
		},
		{
			name: "promote is refused for an episode older than the peer's last response",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100)
				time.Sleep(5 * time.Millisecond)

				// The peer answered after this episode began, so the episode belongs to
				// the healthy window preceding the failure.
				cm.Promote(follower2, time.Now().UnixNano())
				cm.EndRecovery(follower2)
				cm.EndReplication(follower2)
			},
			expectedN: 0,
		},
		{
			name: "cancel discards an unpromoted episode without consuming the single shot",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100)
				cm.Cancel(follower2)

				promotedStart(cm, follower2, 200)
				cm.EndRecovery(follower2)
				cm.EndReplication(follower2)
			},
			expectedN: 2,
		},
		{
			name: "cancel is a no-op once the recovery phase was recorded",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				promotedStart(cm, follower2, 100)
				cm.EndRecovery(follower2)

				cm.Cancel(follower2)
				cm.EndReplication(follower2)
			},
			expectedN: 2,
		},
		{
			name: "replication ending first still emits both phases",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				promotedStart(cm, follower2, 100)
				time.Sleep(5 * time.Millisecond)
				cm.EndReplication(follower2)
			},
			expectedN: 2,
			assertions: func(t *testing.T, lines []string) {
				recPhase, _, _, _ := parseLine(t, lines[0])
				repPhase, _, _, _ := parseLine(t, lines[1])
				assert.Equal(t, "recovery", recPhase)
				assert.Equal(t, "replication", repPhase)
			},
		},
		{
			name: "ending without a start does nothing",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.EndRecovery(follower2)
				cm.EndReplication(follower2)
			},
			expectedN: 0,
		},
		{
			name: "start without end",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				promotedStart(cm, follower2, 100)
				// Don't end the episode - should not panic
			},
			expectedN: 0,
		},
		{
			name: "repeated starts before end avoid overwriting the episode",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				promotedStart(cm, follower2, 100)
				time.Sleep(10 * time.Millisecond)

				// Second Start() call should be ignored due to protection, keeping both
				// the original start instant and the original backlog target.
				cm.Start(follower2, 900)
				target, ok := cm.Target(follower2)
				assert.True(t, ok)
				assert.Equal(t, uint64(100), target)

				cm.EndRecovery(follower2)
				cm.EndReplication(follower2)
			},
			expectedN: 2,
			assertions: func(t *testing.T, lines []string) {
				_, _, _, recDur := parseLine(t, lines[0])
				assert.Greater(t, recDur, int64(10*time.Millisecond),
					"the duration must be measured from the first Start()")
			},
		},
		{
			name: "two followers are tracked independently until one completes",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				promotedStart(cm, follower2, 100)
				promotedStart(cm, follower3, 200)

				cm.EndRecovery(follower3)
				cm.EndReplication(follower3)

				// follower3 disarmed the measurement, so follower2's in-flight episode is
				// dropped instead of recorded.
				cm.EndRecovery(follower2)
				cm.EndReplication(follower2)
			},
			expectedN: 2,
			assertions: func(t *testing.T, lines []string) {
				for _, line := range lines {
					_, id, _, _ := parseLine(t, line)
					assert.Equal(t, follower3, id)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cm, recorded := newTestMsr(t)
			tt.measurement(t, cm)

			lines := recorded()
			require.Len(t, lines, tt.expectedN)

			if tt.assertions != nil {
				tt.assertions(t, lines)
			}
		})
	}
}

func TestCatchUpMsr_EpisodeState(t *testing.T) {
	cm, recorded := newTestMsr(t)

	assert.False(t, cm.IsActive(follower2))
	assert.False(t, cm.IsPromoted(follower2))
	assert.False(t, cm.IsDisarmed())
	assert.Empty(t, cm.ActiveFollowers())

	_, ok := cm.Target(follower2)
	assert.False(t, ok)

	cm.Start(follower2, 42)
	assert.True(t, cm.IsActive(follower2))
	assert.False(t, cm.IsPromoted(follower2), "the peer silence gate has not tripped yet")
	assert.ElementsMatch(t, []uint64{follower2}, cm.ActiveFollowers())

	target, ok := cm.Target(follower2)
	assert.True(t, ok)
	assert.Equal(t, uint64(42), target)

	cm.Promote(follower2, 0)
	assert.True(t, cm.IsPromoted(follower2))

	cm.Start(follower3, 43)
	assert.ElementsMatch(t, []uint64{follower2, follower3}, cm.ActiveFollowers())

	cm.Cancel(follower3)
	assert.False(t, cm.IsActive(follower3))
	assert.ElementsMatch(t, []uint64{follower2}, cm.ActiveFollowers())

	cm.EndRecovery(follower2)
	assert.True(t, cm.IsActive(follower2), "the episode lives on to time its replication")
	assert.False(t, cm.IsDisarmed())

	cm.EndReplication(follower2)
	assert.False(t, cm.IsActive(follower2))
	assert.True(t, cm.IsDisarmed())
	assert.Empty(t, cm.ActiveFollowers())

	cm.Start(follower3, 44)
	assert.False(t, cm.IsActive(follower3), "a disarmed measurer must not open new episodes")

	assert.Len(t, recorded(), 2)
}

func TestCatchUpMsr_ResponseTracking(t *testing.T) {
	cm, _ := newTestMsr(t)

	assert.Equal(t, testSilence, cm.SilenceThreshold())

	_, ok := cm.LastResponse(follower2)
	assert.False(t, ok, "a voter never heard from must not look silent")

	before := time.Now().UnixNano()
	cm.MarkResponse(follower2)

	ts, ok := cm.LastResponse(follower2)
	assert.True(t, ok)
	assert.GreaterOrEqual(t, ts, before)

	cm.MarkResponse(follower2)
	next, _ := cm.LastResponse(follower2)
	assert.GreaterOrEqual(t, next, ts, "later responses must move the timestamp forward")
}

func TestCatchUpMsr_Debug(t *testing.T) {
	cm, recorded := newTestMsr(t)

	info := experiment.CatchUpDebugInfo{
		FollowerID:       follower2,
		LogEntries:       9,
		TargetIndex:      18,
		LeaderFirstIndex: 10,
		LeaderLastIndex:  18,
		LeaderCommitted:  17,
		LeaderApplied:    16,
		FollowerMatch:    18,
		FollowerNext:     19,
	}

	promotedStart(cm, follower2, info.TargetIndex)
	cm.EndRecoveryDebug(follower2, info)

	info.LeaderLastIndex = 24
	info.FollowerMatch = 20
	info.FollowerNext = 21
	cm.EndReplicationDebug(follower2, info)

	lines := recorded()
	require.Len(t, lines, 2)

	assert.True(t, strings.HasPrefix(lines[0], "recovery:2:"), "got: %s", lines[0])
	assert.True(t, strings.HasPrefix(lines[1], "replication:2:"), "got: %s", lines[1])

	for _, want := range []string{
		"[",
		"]",
		"logEntries:9",
		"target:18",
		"logleader:{firstIndex:10, lastIndex:18, committed:17, applied:16}",
		"follower:{id:2, match:18, next:19}",
	} {
		assert.Contains(t, lines[0], want)
	}
	assert.Contains(t, lines[1], "target:18")
	assert.Contains(t, lines[1], "follower:{id:2, match:20, next:21}")
}
