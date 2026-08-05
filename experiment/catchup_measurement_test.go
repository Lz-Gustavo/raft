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
)

// newTestMsr returns a measurer armed from the start with no recording window — the default
// of any run that configures neither — along with a reader closing over its file.
func newTestMsr(t *testing.T) (*experiment.CatchUpMsr, func() []string) {
	t.Helper()
	return newArmedTestMsr(t, 0, 0)
}

// newArmedTestMsr returns a measurer recording only episodes that open in
// [armNs, armNs+windowNs), writing to a temporary file, along with a reader closing over that
// file that flushes and splits the recorded lines. A zero windowNs leaves the window
// unbounded above.
func newArmedTestMsr(t *testing.T, armNs, windowNs int64) (*experiment.CatchUpMsr, func() []string) {
	t.Helper()

	fn := filepath.Join(t.TempDir(), "test-measurement.out")
	cm, err := experiment.NewCatchUpMsr(fn, armNs, windowNs)
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

// idsOf collects the follower each recorded line belongs to, in file order.
func idsOf(t *testing.T, lines []string) []uint64 {
	t.Helper()

	ids := make([]uint64, 0, len(lines))
	for _, line := range lines {
		_, id, _, _ := parseLine(t, line)
		ids = append(ids, id)
	}
	return ids
}

// phasesOf collects the phase tag of each recorded line, in file order.
func phasesOf(t *testing.T, lines []string) []string {
	t.Helper()

	phases := make([]string, 0, len(lines))
	for _, line := range lines {
		phase, _, _, _ := parseLine(t, line)
		phases = append(phases, phase)
	}
	return phases
}

// nowNs stands in for the end instant stepLeader samples once per response, before any of
// the recording I/O, and hands to both phase ends.
func nowNs() int64 {
	return time.Now().UnixNano()
}

func TestCatchUpMsr_Episodes(t *testing.T) {
	tests := []struct {
		name        string
		measurement func(*testing.T, *experiment.CatchUpMsr)
		expectedN   int
		assertions  func(*testing.T, []string)
	}{
		{
			name: "episode records both phases from a single start",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				time.Sleep(5 * time.Millisecond)
				cm.EndRecovery(follower2, nowNs())
				time.Sleep(5 * time.Millisecond)
				cm.EndReplication(follower2, nowNs())
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
			name: "one ack closing both phases records the same duration twice",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				time.Sleep(5 * time.Millisecond)

				// stepLeader samples the end instant once and hands it to both phases, so
				// persisting the recovery line cannot inflate the replication one.
				end := nowNs()
				cm.EndRecovery(follower2, end)
				cm.EndReplication(follower2, end)
			},
			expectedN: 2,
			assertions: func(t *testing.T, lines []string) {
				_, _, _, recDur := parseLine(t, lines[0])
				_, _, _, repDur := parseLine(t, lines[1])
				assert.Equal(t, recDur, repDur, "a shared end instant must yield identical durations")
			},
		},
		{
			name: "each follower records its own episodes",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				cm.EndRecovery(follower2, nowNs())
				cm.EndReplication(follower2, nowNs())

				cm.Start(follower3, 200, 150)
				cm.EndRecovery(follower3, nowNs())
				cm.EndReplication(follower3, nowNs())
			},
			expectedN: 4,
			assertions: func(t *testing.T, lines []string) {
				assert.Equal(t, []uint64{follower2, follower2, follower3, follower3}, idsOf(t, lines))
			},
		},
		{
			// Regression test for the 4-catchup-v3 lan data: load onset provokes a
			// probe/reject cycle on a healthy follower seconds before the failure is
			// injected. Under the old global single-shot latch that early artifact
			// consumed the only recording slot and the real episode on the other
			// follower was never written.
			name: "an early episode on one follower does not suppress a later one on another",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				// The artifact: follower2 opens and completes an episode first.
				cm.Start(follower2, 100, 40)
				cm.EndRecovery(follower2, nowNs())
				cm.EndReplication(follower2, nowNs())

				// The real event: follower3 only becomes quorum-critical later.
				cm.Start(follower3, 5000, 1200)
				assert.True(t, cm.IsActive(follower3), "an unrelated follower must still be measurable")
				cm.EndRecovery(follower3, nowNs())
				cm.EndReplication(follower3, nowNs())
			},
			expectedN: 4,
			assertions: func(t *testing.T, lines []string) {
				assert.Contains(t, idsOf(t, lines), follower3, "the later episode must be recorded")
			},
		},
		{
			// The 6-catchup-v5 failure, in miniature. Under a one-episode-per-follower
			// latch the first line pair here was everything the run recorded, and the
			// second episode — the one provoked by the injected failure — was dropped
			// because its follower had already been retired. 31 of 37 runs lost their
			// real event exactly this way.
			name: "a follower records every episode it opens inside the window",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower3, 100, 40)
				cm.EndRecovery(follower3, nowNs())
				cm.EndReplication(follower3, nowNs())

				cm.Start(follower3, 5000, 1200)
				assert.True(t, cm.IsActive(follower3), "a recorded follower may open another episode")

				target, ok := cm.Target(follower3)
				assert.True(t, ok)
				assert.Equal(t, uint64(5000), target, "the second episode carries its own backlog")

				cm.EndRecovery(follower3, nowNs())
				cm.EndReplication(follower3, nowNs())
			},
			expectedN: 4,
			assertions: func(t *testing.T, lines []string) {
				assert.Equal(t, []uint64{follower3, follower3, follower3, follower3}, idsOf(t, lines))

				_, _, firstStart, _ := parseLine(t, lines[0])
				_, _, secondStart, _ := parseLine(t, lines[2])
				assert.Greater(t, secondStart, firstStart, "each episode is timed from its own start")
			},
		},
		{
			// The episode stays in flight to keep timing its replication, so it — not a
			// retirement latch — is what stops a second recovery line being emitted for
			// the same catch-up.
			name: "an in-flight episode blocks a second recovery line",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				cm.EndRecovery(follower2, nowNs())

				cm.Cancel(follower2)
				cm.Start(follower2, 900, 850)
				cm.EndRecovery(follower2, nowNs())
			},
			expectedN: 1,
			assertions: func(t *testing.T, lines []string) {
				phase, _, _, _ := parseLine(t, lines[0])
				assert.Equal(t, "recovery", phase)
			},
		},
		{
			name: "cancel discards an episode without recording anything",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				cm.Cancel(follower2)
				assert.Zero(t, cm.EpisodeCount(follower2), "a cancelled episode records nothing")

				cm.Start(follower2, 200, 150)
				cm.EndRecovery(follower2, nowNs())
				cm.EndReplication(follower2, nowNs())
			},
			expectedN: 2,
			assertions: func(t *testing.T, lines []string) {
				assert.Equal(t, []uint64{follower2, follower2}, idsOf(t, lines))
			},
		},
		{
			name: "cancel is a no-op once the recovery phase was recorded",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				cm.EndRecovery(follower2, nowNs())

				cm.Cancel(follower2)
				cm.EndReplication(follower2, nowNs())
			},
			expectedN: 2,
		},
		{
			name: "replication ending first still emits both phases",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				time.Sleep(5 * time.Millisecond)
				cm.EndReplication(follower2, nowNs())
			},
			expectedN: 2,
			assertions: func(t *testing.T, lines []string) {
				assert.Equal(t, []string{"recovery", "replication"}, phasesOf(t, lines))
			},
		},
		{
			name: "ending without a start does nothing",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.EndRecovery(follower2, nowNs())
				cm.EndReplication(follower2, nowNs())
			},
			expectedN: 0,
		},
		{
			name: "start without end",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				// Don't end the episode - should not panic
			},
			expectedN: 0,
		},
		{
			name: "repeated starts before end avoid overwriting the episode",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				time.Sleep(10 * time.Millisecond)

				// Second Start() call should be ignored due to protection, keeping the
				// original start instant, backlog target, and follower start index.
				cm.Start(follower2, 900, 850)
				target, ok := cm.Target(follower2)
				assert.True(t, ok)
				assert.Equal(t, uint64(100), target)

				followerStart, ok := cm.FollowerStartIndex(follower2)
				assert.True(t, ok)
				assert.Equal(t, uint64(40), followerStart)

				cm.EndRecovery(follower2, nowNs())
				cm.EndReplication(follower2, nowNs())
			},
			expectedN: 2,
			assertions: func(t *testing.T, lines []string) {
				_, _, _, recDur := parseLine(t, lines[0])
				assert.Greater(t, recDur, int64(10*time.Millisecond),
					"the duration must be measured from the first Start()")
			},
		},
		{
			name: "two followers are tracked concurrently and both record",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				cm.Start(follower3, 200, 150)
				assert.ElementsMatch(t, []uint64{follower2, follower3}, cm.ActiveFollowers())

				cm.EndRecovery(follower3, nowNs())
				cm.EndReplication(follower3, nowNs())

				cm.EndRecovery(follower2, nowNs())
				cm.EndReplication(follower2, nowNs())
			},
			expectedN: 4,
			assertions: func(t *testing.T, lines []string) {
				assert.Equal(t, []uint64{follower3, follower3, follower2, follower2}, idsOf(t, lines))
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

func TestCatchUpMsr_Arming(t *testing.T) {
	t.Run("an arm instant in the future records nothing", func(t *testing.T) {
		cm, recorded := newArmedTestMsr(t, time.Now().Add(time.Hour).UnixNano(), 0)
		assert.False(t, cm.IsArmed())

		cm.Start(follower3, 100, 40)
		assert.False(t, cm.IsActive(follower3), "no episode may open before the arm instant")
		assert.False(t, cm.HasActive())

		cm.EndRecovery(follower3, nowNs())
		cm.EndReplication(follower3, nowNs())

		assert.Empty(t, recorded(), "a run whose arm instant is never reached records nothing")
		assert.Zero(t, cm.EpisodeCount(follower3))
	})

	t.Run("an arm instant in the past measures every episode", func(t *testing.T) {
		cm, recorded := newArmedTestMsr(t, time.Now().Add(-time.Hour).UnixNano(), 0)
		assert.True(t, cm.IsArmed())

		cm.Start(follower2, 100, 40)
		require.True(t, cm.IsActive(follower2))
		cm.EndRecovery(follower2, nowNs())
		cm.EndReplication(follower2, nowNs())

		lines := recorded()
		require.Len(t, lines, 2)
		assert.Equal(t, []uint64{follower2, follower2}, idsOf(t, lines),
			"an already-reached arm instant must behave exactly as an unset one")
	})

	// Regression test for the 5-catchup-v4/100ms data: in 34 of 42 runs the follower that
	// later recovers from the injected failure had already opened and completed an episode
	// of its own at t = 0.3-12 s, provoked by load onset. Keyed by follower and unarmed,
	// that artifact consumed the follower's single slot and the post-kill episode — the one
	// the experiment exists to measure — could never open.
	t.Run("a pre-arm episode does not consume the follower's slot", func(t *testing.T) {
		armNs := time.Now().Add(20 * time.Millisecond).UnixNano()
		cm, recorded := newArmedTestMsr(t, armNs, 0)

		// Load onset, before the failure window: opens and completes, records nothing.
		cm.Start(follower3, 100, 40)
		cm.EndRecovery(follower3, nowNs())
		cm.EndReplication(follower3, nowNs())
		require.Empty(t, recorded(), "a pre-arm episode is not the event being measured")

		time.Sleep(30 * time.Millisecond)
		require.True(t, cm.IsArmed())

		// The injected failure: the same follower becomes quorum-critical once armed.
		cm.Start(follower3, 5000, 1200)
		require.True(t, cm.IsActive(follower3), "the real episode must still be able to open")

		target, ok := cm.Target(follower3)
		assert.True(t, ok)
		assert.Equal(t, uint64(5000), target, "the real episode carries its own backlog")

		cm.EndRecovery(follower3, nowNs())
		cm.EndReplication(follower3, nowNs())

		lines := recorded()
		require.Len(t, lines, 2)
		for _, line := range lines {
			_, id, startNs, _ := parseLine(t, line)
			assert.Equal(t, follower3, id)
			assert.GreaterOrEqual(t, startNs, armNs, "only a post-arm episode may be recorded")
		}
	})
}

func TestCatchUpMsr_Window(t *testing.T) {
	t.Run("an episode opening after the window records nothing", func(t *testing.T) {
		// The window opened and closed an hour ago.
		armNs := time.Now().Add(-2 * time.Hour).UnixNano()
		cm, recorded := newArmedTestMsr(t, armNs, int64(time.Hour))
		assert.False(t, cm.IsArmed(), "the window is the whole of what arming means")

		cm.Start(follower3, 100, 40)
		assert.False(t, cm.IsActive(follower3), "no episode may open once the window closed")

		cm.EndRecovery(follower3, nowNs())
		cm.EndReplication(follower3, nowNs())

		assert.Empty(t, recorded())
	})

	// The property that makes a 10s window enough for a catch-up that runs for half a
	// minute: the bound is on when an episode opens, never on when it finishes.
	t.Run("an episode opening inside the window is recorded however late it ends", func(t *testing.T) {
		window := 200 * time.Millisecond
		cm, recorded := newArmedTestMsr(t, time.Now().UnixNano(), int64(window))

		cm.Start(follower3, 5000, 1200)
		require.True(t, cm.IsActive(follower3))

		// Past the window's end, but inside the grace the seal deadline allows.
		time.Sleep(window + 50*time.Millisecond)
		require.False(t, cm.IsArmed(), "the window itself has closed")

		cm.EndRecovery(follower3, nowNs())
		cm.EndReplication(follower3, nowNs())

		lines := recorded()
		require.Len(t, lines, 2)
		assert.Equal(t, []string{"recovery", "replication"}, phasesOf(t, lines))

		_, _, _, recDur := parseLine(t, lines[0])
		assert.Greater(t, recDur, int64(window), "the episode outlived the window and was still measured")
	})

	t.Run("an episode still in flight at the seal deadline is abandoned", func(t *testing.T) {
		window := 50 * time.Millisecond
		armNs := time.Now().UnixNano()
		cm, recorded := newArmedTestMsr(t, armNs, int64(window))

		cm.Start(follower3, 5000, 1200)
		cm.Observe(follower3, 3000)
		require.True(t, cm.IsActive(follower3))

		// Past arm+2*window, so the episode is written off.
		time.Sleep(3 * window)

		// Any entry point seals; rejections keep arriving well past the window in a run.
		cm.Cancel(follower2)
		assert.False(t, cm.IsActive(follower3), "the seal retires whatever was in flight")

		lines := recorded()
		require.Len(t, lines, 1)
		assert.Equal(t, []string{"abandoned"}, phasesOf(t, lines))
		assert.Equal(t, 1, cm.EpisodeCount(follower3))

		// Charged to the window's end, not to the deadline: the grace is granted to the
		// episode, not billed to it.
		_, _, _, durNs := parseLine(t, lines[0])
		assert.InDelta(t, int64(window), durNs, float64(10*time.Millisecond))

		// The point of the line: how far the follower got before it stopped mattering.
		assert.Contains(t, lines[0], "logEntries:3800")
		assert.Contains(t, lines[0], "acked:1800")
	})

	t.Run("sealing happens once and does not re-record", func(t *testing.T) {
		window := 30 * time.Millisecond
		cm, recorded := newArmedTestMsr(t, time.Now().UnixNano(), int64(window))

		cm.Start(follower2, 100, 40)
		cm.Start(follower3, 200, 150)
		time.Sleep(3 * window)

		cm.Start(follower2, 300, 250)
		cm.EndRecovery(follower2, nowNs())
		cm.EndReplication(follower3, nowNs())
		cm.Cancel(follower3)

		lines := recorded()
		require.Len(t, lines, 2, "one abandoned line per in-flight episode, and no more")
		assert.Equal(t, []string{"abandoned", "abandoned"}, phasesOf(t, lines))
		assert.Equal(t, []uint64{follower2, follower3}, idsOf(t, lines), "sealed in follower order")
	})

	// A window configured without an arm instant would otherwise be anchored at the Unix
	// epoch: closed decades ago, recording nothing, and leaving an empty file that reads
	// exactly like a correct no-failure control. Silent empty files are the specific
	// failure this whole line of work exists to stop producing.
	t.Run("a window with no arm instant runs from now, not from the epoch", func(t *testing.T) {
		cm, recorded := newArmedTestMsr(t, 0, int64(time.Hour))
		assert.True(t, cm.IsArmed(), "an unset arm instant means armed from the process start")

		cm.Start(follower3, 100, 40)
		require.True(t, cm.IsActive(follower3))
		cm.EndRecovery(follower3, nowNs())
		cm.EndReplication(follower3, nowNs())

		assert.Len(t, recorded(), 2)
	})

	t.Run("an unbounded window never seals", func(t *testing.T) {
		cm, recorded := newArmedTestMsr(t, time.Now().Add(-time.Hour).UnixNano(), 0)

		cm.Start(follower3, 5000, 1200)
		time.Sleep(20 * time.Millisecond)

		cm.Cancel(follower2)
		assert.True(t, cm.IsActive(follower3), "with no window there is no deadline to write it off at")
		assert.Empty(t, recorded())
	})
}

func TestCatchUpMsr_EpisodeState(t *testing.T) {
	cm, recorded := newTestMsr(t)

	assert.True(t, cm.IsArmed(), "an unset arm instant measures from the start")
	assert.False(t, cm.IsActive(follower2))
	assert.Zero(t, cm.EpisodeCount(follower2))
	assert.False(t, cm.HasActive())
	assert.Empty(t, cm.ActiveFollowers())

	_, ok := cm.Target(follower2)
	assert.False(t, ok)

	_, ok = cm.FollowerStartIndex(follower2)
	assert.False(t, ok)

	cm.Start(follower2, 42, 20)
	assert.True(t, cm.IsActive(follower2))
	assert.True(t, cm.HasActive())
	assert.ElementsMatch(t, []uint64{follower2}, cm.ActiveFollowers())

	target, ok := cm.Target(follower2)
	assert.True(t, ok)
	assert.Equal(t, uint64(42), target)

	followerStart, ok := cm.FollowerStartIndex(follower2)
	assert.True(t, ok)
	assert.Equal(t, uint64(20), followerStart)

	cm.Start(follower3, 43, 21)
	assert.ElementsMatch(t, []uint64{follower2, follower3}, cm.ActiveFollowers())

	cm.Cancel(follower3)
	assert.False(t, cm.IsActive(follower3))
	assert.Zero(t, cm.EpisodeCount(follower3), "a cancelled episode contributes nothing")
	assert.ElementsMatch(t, []uint64{follower2}, cm.ActiveFollowers())

	cm.EndRecovery(follower2, nowNs())
	assert.True(t, cm.IsActive(follower2), "the episode lives on to time its replication")
	assert.Zero(t, cm.EpisodeCount(follower2), "an episode counts only once it is closed out")

	cm.EndReplication(follower2, nowNs())
	assert.False(t, cm.IsActive(follower2))
	assert.Equal(t, 1, cm.EpisodeCount(follower2))
	assert.False(t, cm.HasActive())
	assert.Empty(t, cm.ActiveFollowers())

	cm.Start(follower2, 44, 22)
	assert.True(t, cm.IsActive(follower2), "a follower keeps recording for as long as the window is open")

	cm.Start(follower3, 44, 22)
	assert.True(t, cm.IsActive(follower3), "another follower stays measurable")

	assert.Len(t, recorded(), 2)
}

func TestCatchUpMsr_Debug(t *testing.T) {
	cm, recorded := newTestMsr(t)

	info := experiment.CatchUpDebugInfo{
		FollowerID:       follower2,
		LogEntries:       9,
		AckedEntries:     0,
		TargetIndex:      18,
		CommitStallNs:    125_000_000,
		LeaderFirstIndex: 10,
		LeaderLastIndex:  18,
		LeaderCommitted:  17,
		LeaderApplied:    16,
		FollowerMatch:    9,
		FollowerNext:     10,
	}

	// followerStartIndex of 9 mirrors info.LogEntries: 18 - 9 = 9, i.e. what
	// catchUpDebugInfo in raft.go would compute for this target/start pair.
	cm.StartDebug(follower2, info.TargetIndex, 9, info)

	info.AckedEntries = 9
	info.FollowerMatch = 18
	info.FollowerNext = 19
	cm.EndRecoveryDebug(follower2, nowNs(), info)

	info.LeaderLastIndex = 24
	info.AckedEntries = 11
	info.FollowerMatch = 20
	info.FollowerNext = 21

	// The stall is a property of the episode, so a later value handed in here must not
	// reach the line: every line of one episode reports the stall as it was at its start.
	info.CommitStallNs = 999
	cm.EndReplicationDebug(follower2, nowNs(), info)

	lines := recorded()
	require.Len(t, lines, 2)

	assert.True(t, strings.HasPrefix(lines[0], "recovery:2:"), "got: %s", lines[0])
	assert.True(t, strings.HasPrefix(lines[1], "replication:2:"), "got: %s", lines[1])

	for _, want := range []string{
		"[",
		"]",
		"logEntries:9",
		"acked:9",
		"target:18",
		"commitStallNs:125000000",
		"logleader:{firstIndex:10, lastIndex:18, committed:17, applied:16}",
		"follower:{id:2, match:18, next:19}",
	} {
		assert.Contains(t, lines[0], want)
	}
	assert.Contains(t, lines[1], "target:18")
	assert.Contains(t, lines[1], "acked:11")
	assert.Contains(t, lines[1], "follower:{id:2, match:20, next:21}")
	assert.Contains(t, lines[1], "commitStallNs:125000000",
		"the stall must be the episode's, not the one passed at the end")
}
