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
	maxEpisodesPerFollower  = 512
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

// NOTE (Gus): a single in-flight catch-up episode for one follower.
type catchUpEpisode struct {
	startNs            int64
	targetIndex        uint64
	followerStartIndex uint64

	// how long the leader's commit index had been frozen when this episode opened
	commitStallNs int64

	lastMatch    uint64
	recoveryDone bool
}

// NOTE (Gus): the measurement records every episode of every follower that begins inside the
// recording window, and leaves the choice of which one the injected failure caused to a later
// analysis. Zero armNs means armed from the start and zero windowNs means no upper bound,
// which together are the behaviour of any run not configuring either. A run that injects no
// failure at all still measures every episode it sees.
type CatchUpMsr struct {
	active   map[uint64]*catchUpEpisode
	recorded map[uint64]int
	armNs    int64
	windowNs int64
	sealed   bool

	buff *bytes.Buffer
	file *os.File
}

func NewCatchUpMsr(fn string, armNs, windowNs int64) (*CatchUpMsr, error) {
	// a window with no arm instant would otherwise be anchored at the Unix epoch
	if armNs <= 0 && windowNs > 0 {
		armNs = time.Now().UnixNano()
	}

	cm := &CatchUpMsr{
		active:   make(map[uint64]*catchUpEpisode),
		recorded: make(map[uint64]int),
		armNs:    armNs,
		windowNs: windowNs,
		buff:     &bytes.Buffer{},
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

// sealDeadlineNs sets a tolerance windown for in flight measurements. It sits a full window past
// the window's end rather than at it, because an episode opening at the very last instant of the
// window is entitled to the same time to complete as one opening at its start.
func (cm *CatchUpMsr) sealDeadlineNs() int64 {
	return cm.armNs + 2*cm.windowNs
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
	slices.Sort(ids)

	for _, id := range ids {
		ep := cm.active[id]
		delete(cm.active, id)
		if !cm.inWindow(ep.startNs) {
			continue
		}

		// The window's end, not the deadline: the extra window is grace granted to the
		// episode, not time it should be charged for.
		info := cm.abandonedInfo(id, ep)
		cm.record(catchupPhaseAbandoned, id, ep, cm.armNs+cm.windowNs, &info)
		cm.recorded[id]++
	}
}

func (cm *CatchUpMsr) abandonedInfo(id uint64, ep *catchUpEpisode) CatchUpDebugInfo {
	var logEntries, acked uint64
	if ep.targetIndex >= ep.followerStartIndex {
		logEntries = ep.targetIndex - ep.followerStartIndex
	}
	if ep.lastMatch >= ep.followerStartIndex {
		acked = ep.lastMatch - ep.followerStartIndex
	}

	return CatchUpDebugInfo{
		FollowerID:    id,
		LogEntries:    logEntries,
		AckedEntries:  acked,
		TargetIndex:   ep.targetIndex,
		FollowerMatch: ep.lastMatch,
	}
}

func (cm *CatchUpMsr) Start(id uint64, targetIndex uint64, followerStartIndex uint64) {
	cm.start(id, targetIndex, followerStartIndex, nil)
}

func (cm *CatchUpMsr) StartDebug(id uint64, targetIndex uint64, followerStartIndex uint64, info CatchUpDebugInfo) {
	cm.start(id, targetIndex, followerStartIndex, &info)
}

func (cm *CatchUpMsr) start(id uint64, targetIndex uint64, followerStartIndex uint64, info *CatchUpDebugInfo) {
	cm.sealWindow()

	if cm.recorded[id] >= maxEpisodesPerFollower {
		return
	}

	now := time.Now().UnixNano()
	if !cm.inWindow(now) {
		return
	}

	if ep, ok := cm.active[id]; ok {
		// an episode opened outside the window is noise that would otherwise hold id's slot
		// for the whole run, so it is replaced rather than kept. One opened inside it is a
		// catch-up in progress and is left alone.
		if cm.inWindow(ep.startNs) {
			return
		}
		delete(cm.active, id)
	}

	ep := &catchUpEpisode{
		startNs:            now,
		targetIndex:        targetIndex,
		followerStartIndex: followerStartIndex,
		lastMatch:          followerStartIndex,
	}
	if info != nil {
		ep.commitStallNs = info.CommitStallNs
	}
	cm.active[id] = ep
}

func (cm *CatchUpMsr) Observe(id uint64, match uint64) {
	ep, ok := cm.active[id]
	if !ok || match <= ep.lastMatch {
		return
	}
	ep.lastMatch = match
}

func (cm *CatchUpMsr) Cancel(id uint64) {
	cm.sealWindow()

	ep, ok := cm.active[id]
	if !ok || ep.recoveryDone {
		return
	}
	delete(cm.active, id)
}

// IsActive reports whether a measurement is in flight for follower id.
func (cm *CatchUpMsr) IsActive(id uint64) bool {
	_, ok := cm.active[id]
	return ok
}

// EpisodeCount returns how many episodes follower id has already contributed to this run's output. An
// episode counts once it is closed out, whether by its replication phase or by being abandoned
// at the seal deadline.
func (cm *CatchUpMsr) EpisodeCount(id uint64) int {
	return cm.recorded[id]
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

// ActiveFollowers returnsa a snapshot of follower IDs currently being tracked, used to reassess
// criticality after every progress update.
func (cm *CatchUpMsr) ActiveFollowers() []uint64 {
	ids := make([]uint64, 0, len(cm.active))
	for id := range cm.active {
		ids = append(ids, id)
	}
	return ids
}

// Target returns the leader last index snapshotted when id's episode started, and whether an
// episode is in flight at all.
func (cm *CatchUpMsr) Target(id uint64) (uint64, bool) {
	ep, ok := cm.active[id]
	if !ok {
		return 0, false
	}
	return ep.targetIndex, true
}

// FollowerStartIndex determines the follower's own last index snapshotted when id's episode started, and
// whether an episode is in flight at all. See the Start doc comment for why this matters.
func (cm *CatchUpMsr) FollowerStartIndex(id uint64) (uint64, bool) {
	ep, ok := cm.active[id]
	if !ok {
		return 0, false
	}
	return ep.followerStartIndex, true
}

// EndRecovery closes the recovery phase — the follower is quorum-critical no more and
// pending client requests can be replied to again. The episode itself stays in flight to
// keep timing the replication of its backlog. endNs is the instant the phase ended, taken
// by the caller rather than here: recording a line fsyncs it, so a clock read at this depth
// would charge the write of one phase to the duration of the next.
func (cm *CatchUpMsr) EndRecovery(id uint64, endNs int64) {
	cm.endRecovery(id, endNs, nil)
}

func (cm *CatchUpMsr) EndRecoveryDebug(id uint64, endNs int64, info CatchUpDebugInfo) {
	cm.endRecovery(id, endNs, &info)
}

func (cm *CatchUpMsr) EndReplication(id uint64, endNs int64) {
	cm.endReplication(id, endNs, nil)
}

func (cm *CatchUpMsr) EndReplicationDebug(id uint64, endNs int64, info CatchUpDebugInfo) {
	cm.endReplication(id, endNs, &info)
}

// dropIfOutsideWindow discards an episode that opened outside the recording window and reports having
// done so. Defensive, for the backwards-clock case start guards against: an episode timed from
// outside the window is not an event this run is measuring, and reaching an end path is no
// reason to record it. The follower keeps its slot — the episode worth recording is still to
// come, which is the whole point of the window.
func (cm *CatchUpMsr) dropIfOutsideWindow(id uint64, ep *catchUpEpisode) bool {
	if cm.inWindow(ep.startNs) {
		return false
	}
	delete(cm.active, id)
	return true
}

func (cm *CatchUpMsr) endRecovery(id uint64, endNs int64, info *CatchUpDebugInfo) {
	cm.sealWindow()

	ep, ok := cm.active[id]
	if !ok || ep.recoveryDone {
		return
	}
	if cm.dropIfOutsideWindow(id, ep) {
		return
	}

	cm.record(catchupPhaseRecovery, id, ep, endNs, info)
	ep.recoveryDone = true
}

func (cm *CatchUpMsr) endReplication(id uint64, endNs int64, info *CatchUpDebugInfo) {
	cm.sealWindow()

	ep, ok := cm.active[id]
	if !ok {
		return
	}
	if cm.dropIfOutsideWindow(id, ep) {
		return
	}

	// NOTE (Gus): the follower can replicate the whole backlog before any commit crossed
	// quorum on its behalf, so emit the pending recovery line here to keep both phases of
	// an episode always present in the output.
	if !ep.recoveryDone {
		cm.record(catchupPhaseRecovery, id, ep, endNs, info)
		ep.recoveryDone = true
	}

	cm.record(catchupPhaseReplication, id, ep, endNs, info)
	delete(cm.active, id)
	cm.recorded[id]++
}

func (cm *CatchUpMsr) record(phase string, id uint64, ep *catchUpEpisode, endNs int64, info *CatchUpDebugInfo) {
	dur := endNs - ep.startNs

	var err error
	if info == nil {
		_, err = fmt.Fprintf(cm.buff, catchupMeasurementFmt, phase, id, ep.startNs, dur)
	} else {
		// The stall belongs to the episode, not to the instant a phase closed, so it is
		// stamped here rather than trusted from the caller — every line of one episode
		// reports the same value.
		stamped := *info
		stamped.CommitStallNs = ep.commitStallNs
		_, err = fmt.Fprintf(cm.buff, catchupMeasurementDebugFmt, phase, id, ep.startNs, dur, formatCatchUpDebugInfo(stamped))
	}
	if err != nil {
		log.Fatalln("failed recording", phase, "duration, err:", err)
	}
	cm.Flush()
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
