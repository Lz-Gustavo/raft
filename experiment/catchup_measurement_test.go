package experiment_test

import (
	"fmt"
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
// [armNs, armNs+windowNs). The seal grace is left unset, so the deadline falls back to a second
// window — the 7-catchup-v6 behaviour these window tests were written against.
func newArmedTestMsr(t *testing.T, armNs, windowNs int64) (*experiment.CatchUpMsr, func() []string) {
	t.Helper()
	return newSealTestMsr(t, armNs, windowNs, 0)
}

// newSealTestMsr returns a measurer writing to a temporary file, along with a reader closing
// over that file that flushes and splits the recorded lines. A zero windowNs leaves the window
// unbounded above; a zero sealGraceNs falls back to the window.
func newSealTestMsr(t *testing.T, armNs, windowNs, sealGraceNs int64) (*experiment.CatchUpMsr, func() []string) {
	t.Helper()

	fn := filepath.Join(t.TempDir(), "test-measurement.out")
	cm, err := experiment.NewCatchUpMsr(fn, armNs, windowNs, sealGraceNs)
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

// acked is the leader-side state stepLeader hands down on every response it steps. The
// follower's acknowledged index is the only field most tests care about: it is what decides
// whether an episode reached its target, which is why the snapshot is required whether or not
// debug output is on.
func acked(match uint64) experiment.CatchUpSnapshot {
	return experiment.CatchUpSnapshot{FollowerMatch: match}
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
				cm.EndRecovery(follower2, nowNs(), acked(60))
				time.Sleep(5 * time.Millisecond)
				cm.EndReplication(follower2, nowNs(), acked(100))
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
				cm.EndRecovery(follower2, end, acked(100))
				cm.EndReplication(follower2, end, acked(100))
			},
			expectedN: 2,
			assertions: func(t *testing.T, lines []string) {
				_, _, _, recDur := parseLine(t, lines[0])
				_, _, _, repDur := parseLine(t, lines[1])
				assert.Equal(t, recDur, repDur, "a shared end instant must yield identical durations")
			},
		},
		{
			name: "an ack short of the target leaves the episode in flight",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				cm.EndReplication(follower2, nowNs(), acked(99))
				assert.True(t, cm.IsActive(follower2), "the backlog is not replicated yet")

				cm.EndReplication(follower2, nowNs(), acked(100))
				assert.False(t, cm.IsActive(follower2))
			},
			expectedN: 2,
			assertions: func(t *testing.T, lines []string) {
				assert.Equal(t, []string{"recovery", "replication"}, phasesOf(t, lines))
			},
		},
		{
			name: "each follower records its own episodes",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				cm.EndRecovery(follower2, nowNs(), acked(100))
				cm.EndReplication(follower2, nowNs(), acked(100))

				cm.Start(follower3, 200, 150)
				cm.EndRecovery(follower3, nowNs(), acked(200))
				cm.EndReplication(follower3, nowNs(), acked(200))
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
				cm.EndRecovery(follower2, nowNs(), acked(100))
				cm.EndReplication(follower2, nowNs(), acked(100))

				// The real event: follower3 only becomes quorum-critical later.
				cm.Start(follower3, 5000, 1200)
				assert.True(t, cm.IsActive(follower3), "an unrelated follower must still be measurable")
				cm.EndRecovery(follower3, nowNs(), acked(5000))
				cm.EndReplication(follower3, nowNs(), acked(5000))
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
				cm.EndRecovery(follower3, nowNs(), acked(100))
				cm.EndReplication(follower3, nowNs(), acked(100))

				cm.Start(follower3, 5000, 1200)
				assert.True(t, cm.IsActive(follower3), "a recorded follower may open another episode")

				cm.EndRecovery(follower3, nowNs(), acked(5000))
				cm.EndReplication(follower3, nowNs(), acked(5000))
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
			name: "cancel discards an episode without recording anything",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				cm.Cancel(follower2)
				assert.Zero(t, cm.EpisodeCount(follower2), "a cancelled episode records nothing")
				assert.False(t, cm.IsActive(follower2))

				cm.Start(follower2, 200, 150)
				cm.EndRecovery(follower2, nowNs(), acked(200))
				cm.EndReplication(follower2, nowNs(), acked(200))
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
				cm.EndRecovery(follower2, nowNs(), acked(60))

				cm.Cancel(follower2)
				assert.True(t, cm.IsActive(follower2), "it is tracking pure replication now")
				cm.EndReplication(follower2, nowNs(), acked(100))
			},
			expectedN: 2,
		},
		{
			name: "cancel spares recovered episodes and drops the rest",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				cm.EndRecovery(follower2, nowNs(), acked(60))

				cm.Start(follower2, 200, 60)
				require.Equal(t, 2, cm.OpenEpisodeCount(follower2))

				cm.Cancel(follower2)
				assert.Equal(t, 1, cm.OpenEpisodeCount(follower2),
					"only the episode still waiting on quorum is discarded")

				cm.EndReplication(follower2, nowNs(), acked(100))
			},
			expectedN: 2,
		},
		{
			name: "replication ending first still emits both phases",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				time.Sleep(5 * time.Millisecond)
				cm.EndReplication(follower2, nowNs(), acked(100))
			},
			expectedN: 2,
			assertions: func(t *testing.T, lines []string) {
				assert.Equal(t, []string{"recovery", "replication"}, phasesOf(t, lines))
			},
		},
		{
			name: "ending without a start does nothing",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.EndRecovery(follower2, nowNs(), acked(100))
				cm.EndReplication(follower2, nowNs(), acked(100))
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
			name: "two followers are tracked concurrently and both record",
			measurement: func(t *testing.T, cm *experiment.CatchUpMsr) {
				cm.Start(follower2, 100, 40)
				cm.Start(follower3, 200, 150)
				assert.ElementsMatch(t, []uint64{follower2, follower3}, cm.ActiveFollowers())

				cm.EndRecovery(follower3, nowNs(), acked(200))
				cm.EndReplication(follower3, nowNs(), acked(200))

				cm.EndRecovery(follower2, nowNs(), acked(100))
				cm.EndReplication(follower2, nowNs(), acked(100))
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

// TestCatchUpMsr_ConcurrentEpisodes is the regression test for the 7-catchup-v6 round, where 19
// of 78 runs recorded nothing for the follower whose recovery the experiment exists to measure.
//
// The recorder kept one *open* episode per follower and dropped any rejection arriving while it
// was in flight. Under load the delayed follower opens an episode within milliseconds of the
// window opening, holding a backlog it cannot clear for tens of seconds — in the real data,
// 141k to 731k entries — so it was still holding the slot several seconds later when the
// failure landed, and the episode the analysis needed was never created. No placement of the
// window fixes that: the block is concurrency, not timing.
func TestCatchUpMsr_ConcurrentEpisodes(t *testing.T) {
	t.Run("a stuck episode does not stop a later one from opening", func(t *testing.T) {
		cm, recorded := newTestMsr(t)

		// The blip: opens at the top of the window against a backlog it will never clear.
		cm.Start(follower3, 512003, 40)
		cm.EndReplication(follower3, nowNs(), acked(56945))
		require.True(t, cm.IsActive(follower3))
		require.Equal(t, 1, cm.OpenEpisodeCount(follower3))

		time.Sleep(5 * time.Millisecond)

		// The injected failure, several seconds later in a real run. Under v6 this Start was
		// dropped on the floor and the run recorded nothing at or after the kill.
		cm.Start(follower3, 512500, 56945)
		require.Equal(t, 2, cm.OpenEpisodeCount(follower3),
			"the second episode must open while the first is still in flight")

		// The follower finally catches up, closing both — each timed from its own start.
		end := nowNs()
		cm.EndRecovery(follower3, end, acked(512500))
		cm.EndReplication(follower3, end, acked(512500))

		lines := recorded()
		require.Len(t, lines, 4)
		assert.Equal(t, []string{"recovery", "recovery", "replication", "replication"},
			phasesOf(t, lines))

		_, _, blipStart, blipDur := parseLine(t, lines[2])
		_, _, realStart, realDur := parseLine(t, lines[3])
		assert.Greater(t, realStart, blipStart, "each episode carries its own start instant")
		assert.Greater(t, blipDur, realDur,
			"the older episode has been running longer, so it reports the larger duration")
	})

	// The outcome that actually matters at the saturated load points: neither episode ever
	// completes, but the later one still exists and is still timestamped after the kill, so the
	// offline selection has something to find. v6 produced a single line here, timestamped
	// before the kill, and the run was scored as a miss.
	t.Run("both stuck episodes are abandoned under their own start instants", func(t *testing.T) {
		window := 40 * time.Millisecond
		cm, recorded := newSealTestMsr(t, time.Now().UnixNano(), int64(window), int64(window))

		cm.Start(follower3, 512003, 40)
		time.Sleep(5 * time.Millisecond)
		cm.Start(follower3, 512500, 40)
		cm.EndReplication(follower3, nowNs(), acked(56945))

		time.Sleep(3 * window)
		cm.Cancel(follower2) // any entry point seals

		lines := recorded()
		require.Len(t, lines, 2)
		assert.Equal(t, []string{"abandoned", "abandoned"}, phasesOf(t, lines))
		assert.Equal(t, 2, cm.EpisodeCount(follower3))

		_, _, firstStart, _ := parseLine(t, lines[0])
		_, _, secondStart, _ := parseLine(t, lines[1])
		assert.Greater(t, secondStart, firstStart,
			"the later episode is what a post-kill selection has to be able to find")

		// How far the follower actually got, which is the whole point of an abandoned line.
		assert.Contains(t, lines[0], "logEntries:511963")
		assert.Contains(t, lines[0], "acked:56905")
	})
}

// TestCatchUpMsr_RecoveryClosesAllPending covers the soundness argument for settling every open
// episode at once: the caller only reaches that path when maybeCommit() returned true, and the
// sole progress mutation in that branch is this follower's, so the acknowledgement that restored
// quorum restored it for every episode of that follower still waiting on it.
func TestCatchUpMsr_RecoveryClosesAllPending(t *testing.T) {
	cm, recorded := newTestMsr(t)

	cm.Start(follower3, 100, 40)
	cm.Start(follower3, 200, 60)
	cm.Start(follower3, 300, 80)
	cm.Start(follower2, 900, 500)

	cm.EndRecovery(follower3, nowNs(), acked(90))

	lines := recorded()
	require.Len(t, lines, 3, "one recovery line per open episode of that follower")
	assert.Equal(t, []string{"recovery", "recovery", "recovery"}, phasesOf(t, lines))
	assert.Equal(t, []uint64{follower3, follower3, follower3}, idsOf(t, lines),
		"another follower's episodes are untouched")
	assert.Equal(t, 3, cm.OpenEpisodeCount(follower3), "they live on to time their replication")

	// A second commit-advancing ack must not re-emit any of them.
	cm.EndRecovery(follower3, nowNs(), acked(95))
	assert.Len(t, recorded(), 3)

	// The open set is a recoveryDone prefix followed by a pending suffix — the property that
	// lets endRecovery find its work by walking back from the end instead of keeping a counter.
	// An episode opening after a recovery lands in the suffix, and only it is emitted next.
	cm.Start(follower3, 400, 90)
	cm.EndRecovery(follower3, nowNs(), acked(100))

	lines = recorded()
	require.Len(t, lines, 4, "only the episode opened after the last recovery is emitted")
	_, _, lastStart, _ := parseLine(t, lines[3])
	_, _, thirdStart, _ := parseLine(t, lines[2])
	assert.Greater(t, lastStart, thirdStart, "and it is the newest one")

	// Cancel drops the pending suffix and keeps the prefix, so the set stays partitioned.
	cm.Start(follower3, 500, 95)
	cm.Cancel(follower3)
	assert.Equal(t, 4, cm.OpenEpisodeCount(follower3), "the recovered episodes survive a cancel")

	cm.EndRecovery(follower3, nowNs(), acked(100))
	assert.Len(t, recorded(), 4, "nothing is pending, so nothing is re-emitted")
}

func TestCatchUpMsr_Arming(t *testing.T) {
	t.Run("an arm instant in the future records nothing", func(t *testing.T) {
		cm, recorded := newArmedTestMsr(t, time.Now().Add(time.Hour).UnixNano(), 0)
		assert.False(t, cm.IsArmed())

		cm.Start(follower3, 100, 40)
		assert.False(t, cm.IsActive(follower3), "no episode may open before the arm instant")
		assert.False(t, cm.HasActive())

		cm.EndRecovery(follower3, nowNs(), acked(100))
		cm.EndReplication(follower3, nowNs(), acked(100))

		assert.Empty(t, recorded(), "a run whose arm instant is never reached records nothing")
		assert.Zero(t, cm.EpisodeCount(follower3))
	})

	t.Run("an arm instant in the past measures every episode", func(t *testing.T) {
		cm, recorded := newArmedTestMsr(t, time.Now().Add(-time.Hour).UnixNano(), 0)
		assert.True(t, cm.IsArmed())

		cm.Start(follower2, 100, 40)
		require.True(t, cm.IsActive(follower2))
		cm.EndRecovery(follower2, nowNs(), acked(100))
		cm.EndReplication(follower2, nowNs(), acked(100))

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
		cm.EndRecovery(follower3, nowNs(), acked(100))
		cm.EndReplication(follower3, nowNs(), acked(100))
		require.Empty(t, recorded(), "a pre-arm episode is not the event being measured")

		time.Sleep(30 * time.Millisecond)
		require.True(t, cm.IsArmed())

		// The injected failure: the same follower becomes quorum-critical once armed.
		cm.Start(follower3, 5000, 1200)
		require.True(t, cm.IsActive(follower3), "the real episode must still be able to open")

		cm.EndRecoveryDebug(follower3, nowNs(), acked(5000))
		cm.EndReplicationDebug(follower3, nowNs(), acked(5000))

		lines := recorded()
		require.Len(t, lines, 2)
		for _, line := range lines {
			_, id, startNs, _ := parseLine(t, line)
			assert.Equal(t, follower3, id)
			assert.GreaterOrEqual(t, startNs, armNs, "only a post-arm episode may be recorded")
		}
		assert.Contains(t, lines[0], "target:5000", "the real episode carries its own backlog")
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

		cm.EndRecovery(follower3, nowNs(), acked(100))
		cm.EndReplication(follower3, nowNs(), acked(100))

		assert.Empty(t, recorded())
	})

	// The property that makes a 15s window enough for a catch-up that runs for half a
	// minute: the bound is on when an episode opens, never on when it finishes.
	t.Run("an episode opening inside the window is recorded however late it ends", func(t *testing.T) {
		window := 200 * time.Millisecond
		cm, recorded := newArmedTestMsr(t, time.Now().UnixNano(), int64(window))

		cm.Start(follower3, 5000, 1200)
		require.True(t, cm.IsActive(follower3))

		// Past the window's end, but inside the grace the seal deadline allows.
		time.Sleep(window + 50*time.Millisecond)
		require.False(t, cm.IsArmed(), "the window itself has closed")

		cm.EndRecovery(follower3, nowNs(), acked(5000))
		cm.EndReplication(follower3, nowNs(), acked(5000))

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
		cm.EndReplication(follower3, nowNs(), acked(3000))
		require.True(t, cm.IsActive(follower3), "still short of its target")

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
		cm.EndRecovery(follower2, nowNs(), acked(300))
		cm.EndReplication(follower3, nowNs(), acked(300))
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
		cm.EndRecovery(follower3, nowNs(), acked(100))
		cm.EndReplication(follower3, nowNs(), acked(100))

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

// TestCatchUpMsr_SealGrace covers the knob that lets the window be widened without pushing the
// deadline past the end of the run. In 7-catchup-v6 the deadline was armNs+2*windowNs, so a
// window long enough to be sure of catching the failure also moved the deadline past the instant
// the harness kills etcd — and the abandoned lines, the only evidence a follower never caught up,
// were never written at all.
func TestCatchUpMsr_SealGrace(t *testing.T) {
	t.Run("the deadline is the window plus the grace, not two windows", func(t *testing.T) {
		window := 200 * time.Millisecond
		grace := 30 * time.Millisecond
		cm, recorded := newSealTestMsr(t, time.Now().UnixNano(), int64(window), int64(grace))

		cm.Start(follower3, 5000, 1200)

		// Past window+grace but far short of the v6 deadline of two windows.
		time.Sleep(window + 2*grace)
		cm.Cancel(follower2)

		lines := recorded()
		require.Len(t, lines, 1, "a short grace must seal well before a second window elapses")
		assert.Equal(t, []string{"abandoned"}, phasesOf(t, lines))
	})

	t.Run("an unset grace falls back to the window", func(t *testing.T) {
		window := 40 * time.Millisecond
		cm, recorded := newSealTestMsr(t, time.Now().UnixNano(), int64(window), 0)

		cm.Start(follower3, 5000, 1200)

		// Past the window, short of arm+2*window: the v6 deadline has not been reached.
		time.Sleep(window + window/2)
		cm.Cancel(follower2)
		require.Empty(t, recorded(), "an unset grace must reproduce the v6 deadline exactly")

		time.Sleep(window)
		cm.Cancel(follower2)
		assert.Len(t, recorded(), 1)
	})
}

// TestCatchUpMsr_UnboundedOpenEpisodes covers the absence of any cap on concurrently open
// episodes. A cap can only be enforced by dropping episodes, and a dropped episode is
// indistinguishable in the output from a catch-up that never happened — which is exactly what
// cost 6-catchup-v5 and 7-catchup-v6 their captures. Every episode that opens inside the window
// is accounted for, either by its own phase lines or by an abandoned one.
func TestCatchUpMsr_UnboundedOpenEpisodes(t *testing.T) {
	const episodes = 512

	window := 50 * time.Millisecond
	cm, recorded := newSealTestMsr(t, time.Now().UnixNano(), int64(window), int64(window))

	for i := 0; i < episodes; i++ {
		cm.Start(follower3, uint64(1000+i), 40)
	}
	assert.Equal(t, episodes, cm.OpenEpisodeCount(follower3), "nothing is dropped to a cap")

	time.Sleep(3 * window)
	cm.Cancel(follower2)

	lines := recorded()
	require.Len(t, lines, episodes, "one abandoned line per episode, and nothing missing")
	assert.Equal(t, episodes, cm.EpisodeCount(follower3))

	// The oldest survives, so the recorded span starts at the first target opened.
	assert.Contains(t, lines[0], fmt.Sprintf("target:%d", 1000))
	assert.Contains(t, lines[episodes-1], fmt.Sprintf("target:%d", 1000+episodes-1))
}

func TestCatchUpMsr_EpisodeState(t *testing.T) {
	cm, recorded := newTestMsr(t)

	assert.True(t, cm.IsArmed(), "an unset arm instant measures from the start")
	assert.False(t, cm.IsActive(follower2))
	assert.Zero(t, cm.EpisodeCount(follower2))
	assert.Zero(t, cm.OpenEpisodeCount(follower2))
	assert.False(t, cm.HasActive())
	assert.Empty(t, cm.ActiveFollowers())

	cm.Start(follower2, 42, 20)
	assert.True(t, cm.IsActive(follower2))
	assert.True(t, cm.HasActive())
	assert.Equal(t, 1, cm.OpenEpisodeCount(follower2))
	assert.ElementsMatch(t, []uint64{follower2}, cm.ActiveFollowers())

	cm.Start(follower3, 43, 21)
	assert.ElementsMatch(t, []uint64{follower2, follower3}, cm.ActiveFollowers())

	cm.Cancel(follower3)
	assert.False(t, cm.IsActive(follower3))
	assert.Zero(t, cm.EpisodeCount(follower3), "a cancelled episode contributes nothing")
	assert.ElementsMatch(t, []uint64{follower2}, cm.ActiveFollowers())

	cm.EndRecovery(follower2, nowNs(), acked(30))
	assert.True(t, cm.IsActive(follower2), "the episode lives on to time its replication")
	assert.Zero(t, cm.EpisodeCount(follower2), "an episode counts only once it is closed out")

	cm.EndReplication(follower2, nowNs(), acked(42))
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

	// followerStartIndex of 9 against a target of 18 is what raft.go's rejection branch would
	// snapshot: logEntries is 18 - 9, derived by the recorder rather than passed in, because one
	// acknowledgement can close several episodes and the caller cannot build a payload per
	// episode when it does not know how many there are.
	cm.Start(follower2, 18, 9)

	cm.EndRecoveryDebug(follower2, nowNs(), experiment.CatchUpSnapshot{
		FollowerID:       follower2,
		LeaderFirstIndex: 10,
		LeaderLastIndex:  18,
		LeaderCommitted:  17,
		LeaderApplied:    16,
		FollowerMatch:    18,
		FollowerNext:     19,
	})

	cm.EndReplicationDebug(follower2, nowNs(), experiment.CatchUpSnapshot{
		FollowerID:       follower2,
		LeaderFirstIndex: 10,
		LeaderLastIndex:  24,
		LeaderCommitted:  17,
		LeaderApplied:    16,
		FollowerMatch:    20,
		FollowerNext:     21,
	})

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
		"logleader:{firstIndex:10, lastIndex:18, committed:17, applied:16}",
		"follower:{id:2, match:18, next:19}",
	} {
		assert.Contains(t, lines[0], want)
	}

	// The target is a property of the episode, so every line of one episode reports it as it was
	// at that episode's start rather than at the instant a phase closed.
	assert.Contains(t, lines[1], "target:18")
	assert.Contains(t, lines[1], "acked:11")
	assert.Contains(t, lines[1], "follower:{id:2, match:20, next:21}")
}

// A run that configures nothing writes bare lines: the debug payload is the only thing the flag
// controls, since the snapshot itself is needed either way to decide when an episode is done.
func TestCatchUpMsr_NoDebugPayload(t *testing.T) {
	cm, recorded := newTestMsr(t)

	cm.Start(follower2, 100, 40)
	cm.EndRecovery(follower2, nowNs(), acked(100))
	cm.EndReplication(follower2, nowNs(), acked(100))

	lines := recorded()
	require.Len(t, lines, 2)
	for _, line := range lines {
		assert.NotContains(t, line, "[", "no payload without the debug flag: %s", line)
	}
}
