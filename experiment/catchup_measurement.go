package experiment

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"slices"
	"time"
)

const (
	catchupMeasurementFmt      = "%s:%d:%d:%d\n"
	catchupMeasurementDebugFmt = "%s:%d:%d:%d %s\n"

	catchupPhaseRecovery    = "recovery"
	catchupPhaseReplication = "replication"
	catchupPhaseAbandoned   = "abandoned"
	catchupPhaseEvicted     = "evicted"

	maxEpisodesPerFollower     = 8192
	maxOpenEpisodesPerFollower = 128
)

type CatchUpDebugInfo struct {
	FollowerID       uint64
	LogEntries       uint64
	AckedEntries     uint64
	TargetIndex      uint64
	CommitStallNs    int64
	LeaderFirstIndex uint64
	LeaderLastIndex  uint64
	LeaderCommitted  uint64
	LeaderApplied    uint64
	FollowerMatch    uint64
	FollowerNext     uint64
}

// NOTE (Gus): the leader-side state at the instant a phase closed. Everything in it is
// independent of *which* episode is being closed, which is the whole point: one acknowledgement
// can close several episodes at once, and it would be wrong for the caller to build a payload
// per episode when it does not know how many there are. The per-episode fields of
// CatchUpDebugInfo — LogEntries, AckedEntries, TargetIndex, CommitStallNs — are derived here,
// from the episode, when each line is written.
type CatchUpSnapshot struct {
	FollowerID       uint64
	LeaderFirstIndex uint64
	LeaderLastIndex  uint64
	LeaderCommitted  uint64
	LeaderApplied    uint64
	FollowerMatch    uint64
	FollowerNext     uint64
}

// NOTE (Gus): a single in-flight catch-up episode for one follower.
type catchUpEpisode struct {
	startNs            int64
	targetIndex        uint64
	followerStartIndex uint64

	// how long the leader's commit index had been frozen when this episode opened
	commitStallNs int64

	recoveryDone bool
}

// NOTE (Gus): the measurement records every episode of every follower that begins inside the
// recording window, and leaves the choice of which one the injected failure caused to a later
// analysis. Zero armNs means armed from the start and zero windowNs means no upper bound,
// which together are the behaviour of any run not configuring either. A run that injects no
// failure at all still measures every episode it sees.
type CatchUpMsr struct {
	active   map[uint64][]*catchUpEpisode
	recorded map[uint64]int
	evicted  map[uint64]int

	// the follower's high-water acknowledged index. Shared by every episode of that follower —
	// they all watch the same pr.Match — and read only when an episode has to be abandoned
	// without an end line to take the follower's state from.
	lastMatch       map[uint64]uint64
	pendingRecovery map[uint64]int

	armNs       int64
	windowNs    int64
	sealGraceNs int64
	sealed      bool

	buff *bytes.Buffer
	file *os.File
}

func NewCatchUpMsr(fn string, armNs, windowNs, sealGraceNs int64) (*CatchUpMsr, error) {
	// a window with no arm instant would otherwise be anchored at the Unix epoch
	if armNs <= 0 && windowNs > 0 {
		armNs = time.Now().UnixNano()
	}

	cm := &CatchUpMsr{
		active:          make(map[uint64][]*catchUpEpisode),
		recorded:        make(map[uint64]int),
		evicted:         make(map[uint64]int),
		lastMatch:       make(map[uint64]uint64),
		pendingRecovery: make(map[uint64]int),
		armNs:           armNs,
		windowNs:        windowNs,
		sealGraceNs:     sealGraceNs,
		buff:            &bytes.Buffer{},
	}

	fd, err := createMeasurementFile(fn)
	if err != nil {
		return nil, fmt.Errorf("could not create catch up measurement file: %w", err)
	}
	cm.file = fd

	return cm, nil
}

// inWindow reports whether an instant falls inside the recording window. Applied to an episode's
// start, never to its end: a catch-up that opens just before the window closes and runs for
// another half minute is exactly the event worth having, so the window bounds when episodes
// may *open* and nothing else.
func (cm *CatchUpMsr) inWindow(ns int64) bool {
	return ns >= cm.armNs && (cm.windowNs == 0 || ns < cm.armNs+cm.windowNs)
}

// sealDeadlineNs sets a tolerance for in-flight measurements: an episode opening at the very last
// instant of the window is entitled to time to complete, so the deadline sits past the window's
// end rather than at it.
func (cm *CatchUpMsr) sealDeadlineNs() int64 {
	grace := cm.sealGraceNs
	if grace <= 0 {
		grace = cm.windowNs
	}
	return cm.armNs + cm.windowNs + grace
}

func (cm *CatchUpMsr) sealWindow() {
	if cm.sealed || cm.windowNs == 0 || time.Now().UnixNano() < cm.sealDeadlineNs() {
		return
	}
	cm.sealed = true

	ids := make([]uint64, 0, len(cm.active))
	for id := range cm.active {
		ids = append(ids, id)
	}
	for id := range cm.evicted {
		if _, ok := cm.active[id]; !ok {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)

	wrote := false
	for _, id := range ids {
		for _, ep := range cm.active[id] {
			if !cm.inWindow(ep.startNs) {
				continue
			}

			// the window's end, not the deadline: the extra grace is granted to the episode,
			// not time it should be charged for.
			info := cm.abandonedInfo(id, ep)
			cm.recordInfo(catchupPhaseAbandoned, id, ep.startNs, cm.armNs+cm.windowNs-ep.startNs, &info)
			cm.recorded[id]++
			wrote = true
		}
		delete(cm.active, id)
		delete(cm.pendingRecovery, id)

		// one summary line rather than one line per eviction
		if n := cm.evicted[id]; n > 0 {
			cm.recordRaw(catchupPhaseEvicted, id, cm.armNs, int64(n))
			wrote = true
		}
	}

	if wrote {
		cm.Flush()
	}
}

func (cm *CatchUpMsr) abandonedInfo(id uint64, ep *catchUpEpisode) CatchUpDebugInfo {
	match := cm.lastMatch[id]
	if match < ep.followerStartIndex {
		match = ep.followerStartIndex
	}

	return CatchUpDebugInfo{
		FollowerID:    id,
		LogEntries:    sub(ep.targetIndex, ep.followerStartIndex),
		AckedEntries:  sub(match, ep.followerStartIndex),
		TargetIndex:   ep.targetIndex,
		CommitStallNs: ep.commitStallNs,
		FollowerMatch: match,
	}
}

// Start opens a catch-up episode for a follower. commitStallNs is how long the leader's commit
// index had been frozen at this instant; it belongs to the episode, so every line the episode
// later produces reports the same value.
func (cm *CatchUpMsr) Start(id uint64, targetIndex, followerStartIndex uint64, commitStallNs int64) {
	cm.sealWindow()

	if cm.recorded[id] >= maxEpisodesPerFollower {
		return
	}

	now := time.Now().UnixNano()
	if !cm.inWindow(now) {
		return
	}

	open := cm.active[id]
	if len(open) >= maxOpenEpisodesPerFollower {
		open = cm.evictOldest(id, open)
	}

	cm.active[id] = append(open, &catchUpEpisode{
		startNs:            now,
		targetIndex:        targetIndex,
		followerStartIndex: followerStartIndex,
		commitStallNs:      commitStallNs,
	})
	cm.pendingRecovery[id]++

	if _, ok := cm.lastMatch[id]; !ok {
		cm.lastMatch[id] = followerStartIndex
	}
}

// evictOldest makes room by dropping the follower's oldest open episode. The oldest is the right
// one to lose: the analysis wants the first episode opening at or after the injected failure, so
// the newest are the candidates worth protecting. Nothing is written here — see the summary line
// in sealWindow.
func (cm *CatchUpMsr) evictOldest(id uint64, open []*catchUpEpisode) []*catchUpEpisode {
	if len(open) == 0 {
		return open
	}
	cm.evicted[id]++
	return open[1:]
}

func (cm *CatchUpMsr) Cancel(id uint64) {
	cm.sealWindow()

	open := cm.active[id]
	if len(open) == 0 {
		return
	}

	// An episode whose recovery was already recorded is tracking pure replication and no longer
	// cares about quorum, so resolving criticality elsewhere is not a reason to discard it.
	kept := open[:0]
	for _, ep := range open {
		if ep.recoveryDone {
			kept = append(kept, ep)
		}
	}
	cm.setOpen(id, kept)
}

// setOpen replaces a follower's open set, dropping the map entry when it empties so HasActive and
// ActiveFollowers stay honest about which followers are actually being tracked.
func (cm *CatchUpMsr) setOpen(id uint64, open []*catchUpEpisode) {
	if len(open) == 0 {
		delete(cm.active, id)
		return
	}
	cm.active[id] = open
}

// IsActive reports whether any measurement is in flight for follower id.
func (cm *CatchUpMsr) IsActive(id uint64) bool {
	return len(cm.active[id]) > 0
}

// OpenEpisodeCount returns how many episodes are in flight for follower id.
func (cm *CatchUpMsr) OpenEpisodeCount(id uint64) int {
	return len(cm.active[id])
}

// EpisodeCount returns how many episodes follower id has already contributed to this run's output. An
// episode counts once it is closed out, whether by its replication phase or by being abandoned
// at the seal deadline.
func (cm *CatchUpMsr) EpisodeCount(id uint64) int {
	return cm.recorded[id]
}

// EvictedCount returns how many of follower id's episodes were dropped to stay under the open
// cap, i.e. how much this run is known to be missing.
func (cm *CatchUpMsr) EvictedCount(id uint64) int {
	return cm.evicted[id]
}

// IsArmed reports whether the recording window is open right now, i.e. whether an episode
// opening at this instant would be measurable at all. Always true when neither an arm instant
// nor a window was configured.
func (cm *CatchUpMsr) IsArmed() bool {
	return cm.inWindow(time.Now().UnixNano())
}

// HasActive reports whether any episode is in flight at all. Lets the leader skip the
// allocation ActiveFollowers() makes, on a path it walks for every response it steps.
func (cm *CatchUpMsr) HasActive() bool {
	return len(cm.active) > 0
}

// ActiveFollowers returns a snapshot of follower IDs currently being tracked, used to reassess
// criticality after every progress update.
func (cm *CatchUpMsr) ActiveFollowers() []uint64 {
	ids := make([]uint64, 0, len(cm.active))
	for id := range cm.active {
		ids = append(ids, id)
	}
	return ids
}

// EndRecovery closes the recovery phase — the follower is quorum-critical no more and
// pending client requests can be replied to again. The episodes themselves stay in flight to
// keep timing the replication of their backlogs. endNs is the instant the phase ended, taken
// by the caller rather than here: a clock read at this depth would charge the write of one
// phase to the duration of the next.
func (cm *CatchUpMsr) EndRecovery(id uint64, endNs int64, snap CatchUpSnapshot) {
	cm.endRecovery(id, endNs, snap, false)
}

func (cm *CatchUpMsr) EndRecoveryDebug(id uint64, endNs int64, snap CatchUpSnapshot) {
	cm.endRecovery(id, endNs, snap, true)
}

func (cm *CatchUpMsr) EndReplication(id uint64, endNs int64, snap CatchUpSnapshot) {
	cm.endReplication(id, endNs, snap, false)
}

func (cm *CatchUpMsr) EndReplicationDebug(id uint64, endNs int64, snap CatchUpSnapshot) {
	cm.endReplication(id, endNs, snap, true)
}

// NOTE (Gus): every open episode of this follower is settled here, not just one. The caller
// only reaches this path when maybeCommit() returned true, and the sole progress mutation in
// that branch is this follower's, so the acknowledgement that restored quorum restored it for
// every episode of that follower that was still waiting on it.
func (cm *CatchUpMsr) endRecovery(id uint64, endNs int64, snap CatchUpSnapshot, debug bool) {
	cm.sealWindow()

	if cm.pendingRecovery[id] == 0 {
		return
	}
	cm.observeMatch(id, snap.FollowerMatch)

	open := cm.active[id]
	kept := open[:0]
	wrote := false
	for _, ep := range open {
		// Defensive, for the backwards-clock case Start guards against: an episode timed from
		// outside the window is not an event this run is measuring, and reaching an end path is
		// no reason to record it.
		if !cm.inWindow(ep.startNs) {
			continue
		}
		if !ep.recoveryDone {
			cm.recordEpisode(catchupPhaseRecovery, id, ep, endNs, snap, debug)
			ep.recoveryDone = true
			wrote = true
		}
		kept = append(kept, ep)
	}
	cm.setOpen(id, kept)
	cm.pendingRecovery[id] = 0

	if wrote {
		cm.Flush()
	}
}

// NOTE (Gus): the open set is ordered by startNs, and therefore by targetIndex too — the
// leader's last index only grows, so an episode opened later can never carry a smaller target.
// The first episode the follower has not reached ends the scan, because no later one can be
// satisfied either. That is what keeps this O(1) in the steady state on a path the leader walks
// for every response it steps, ~25k times a second at the top of the client sweep.
func (cm *CatchUpMsr) endReplication(id uint64, endNs int64, snap CatchUpSnapshot, debug bool) {
	cm.sealWindow()

	open := cm.active[id]
	if len(open) == 0 {
		return
	}
	cm.observeMatch(id, snap.FollowerMatch)

	match := cm.lastMatch[id]
	closed, wrote := 0, false
	for _, ep := range open {
		if !cm.inWindow(ep.startNs) {
			closed++
			continue
		}
		if match < ep.targetIndex {
			break
		}

		// The follower can replicate the whole backlog before any commit crossed quorum on its
		// behalf, so emit the pending recovery line here to keep both phases of an episode
		// always present in the output.
		if !ep.recoveryDone {
			cm.recordEpisode(catchupPhaseRecovery, id, ep, endNs, snap, debug)
			ep.recoveryDone = true
		}
		cm.recordEpisode(catchupPhaseReplication, id, ep, endNs, snap, debug)
		cm.recorded[id]++
		closed++
		wrote = true
	}

	if closed > 0 {
		cm.setOpen(id, open[closed:])
	}
	if wrote {
		cm.Flush()
	}
}

// observeMatch keeps the follower's high-water acknowledged index. One value per follower rather
// than per episode: they all watch the same pr.Match, and each episode subtracts its own start
// index from it. Only an abandoned episode ever reads it, and how far it did get is the whole
// point of that outcome.
func (cm *CatchUpMsr) observeMatch(id uint64, match uint64) {
	if match <= cm.lastMatch[id] {
		return
	}
	cm.lastMatch[id] = match
}

func (cm *CatchUpMsr) recordEpisode(phase string, id uint64, ep *catchUpEpisode, endNs int64, snap CatchUpSnapshot, debug bool) {
	if !debug {
		cm.recordRaw(phase, id, ep.startNs, endNs-ep.startNs)
		return
	}

	info := CatchUpDebugInfo{
		FollowerID:       id,
		LogEntries:       sub(ep.targetIndex, ep.followerStartIndex),
		AckedEntries:     sub(snap.FollowerMatch, ep.followerStartIndex),
		TargetIndex:      ep.targetIndex,
		CommitStallNs:    ep.commitStallNs,
		LeaderFirstIndex: snap.LeaderFirstIndex,
		LeaderLastIndex:  snap.LeaderLastIndex,
		LeaderCommitted:  snap.LeaderCommitted,
		LeaderApplied:    snap.LeaderApplied,
		FollowerMatch:    snap.FollowerMatch,
		FollowerNext:     snap.FollowerNext,
	}
	cm.recordInfo(phase, id, ep.startNs, endNs-ep.startNs, &info)
}

// recordRaw and recordInfo buffer one line each. Neither flushes: a single acknowledgement can
// close many episodes, and one fsync per line would put hundreds of them inside one stepLeader
// call. The caller flushes once before returning, so nothing outlives the hook unpersisted.
func (cm *CatchUpMsr) recordRaw(phase string, id uint64, startNs, value int64) {
	if _, err := fmt.Fprintf(cm.buff, catchupMeasurementFmt, phase, id, startNs, value); err != nil {
		log.Fatalln("failed recording", phase, "duration, err:", err)
	}
}

func (cm *CatchUpMsr) recordInfo(phase string, id uint64, startNs, durNs int64, info *CatchUpDebugInfo) {
	if _, err := fmt.Fprintf(cm.buff, catchupMeasurementDebugFmt, phase, id, startNs, durNs, formatCatchUpDebugInfo(*info)); err != nil {
		log.Fatalln("failed recording", phase, "duration, err:", err)
	}
}

func formatCatchUpDebugInfo(info CatchUpDebugInfo) string {
	return fmt.Sprintf("[logEntries:%d, acked:%d, target:%d, commitStallNs:%d, logleader:{firstIndex:%d, lastIndex:%d, committed:%d, applied:%d}, follower:{id:%d, match:%d, next:%d}]",
		info.LogEntries,
		info.AckedEntries,
		info.TargetIndex,
		info.CommitStallNs,
		info.LeaderFirstIndex,
		info.LeaderLastIndex,
		info.LeaderCommitted,
		info.LeaderApplied,
		info.FollowerID,
		info.FollowerMatch,
		info.FollowerNext,
	)
}

func sub(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}

func (cm *CatchUpMsr) Flush() {
	if _, err := cm.buff.WriteTo(cm.file); err != nil {
		log.Fatalln("failed copying from measurement buffer to file, err:", err)
	}

	if err := cm.file.Sync(); err != nil {
		log.Fatalln("failed flushing data to disk, err:", err)
	}
}

func (cm *CatchUpMsr) Close() {
	cm.file.Close()
}
