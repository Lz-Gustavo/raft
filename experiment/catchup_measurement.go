package experiment

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	catchupMeasurementFmt      = "%s:%d:%d:%d\n"
	catchupMeasurementDebugFmt = "%s:%d:%d:%d %s\n"

	// NOTE (Gus): phase tags leading every recorded line. A single catch-up episode
	// yields one line per phase, both timed from the same start instant, so the
	// replication duration always contains the recovery one.
	catchupPhaseRecovery    = "recovery"
	catchupPhaseReplication = "replication"
)

type CatchUpDebugInfo struct {
	FollowerID       uint64
	LogEntries       uint64
	TargetIndex      uint64
	LeaderFirstIndex uint64
	LeaderLastIndex  uint64
	LeaderCommitted  uint64
	LeaderApplied    uint64
	FollowerMatch    uint64
	FollowerNext     uint64
}

// NOTE (Gus): a single in-flight catch-up episode for one follower. It is only turned
// into recorded output once promoted, i.e. once the peer-silence gate confirmed the
// episode is driven by an actual peer failure and not by routine replication jitter.
type catchUpEpisode struct {
	startNs            int64
	targetIndex        uint64
	followerStartIndex uint64
	promoted           bool
	recoveryDone       bool
}

type CatchUpMsr struct {
	active   map[uint64]*catchUpEpisode
	lastResp map[uint64]int64
	silence  time.Duration
	disarmed bool

	buff *bytes.Buffer
	file *os.File
}

func NewCatchUpMsr(fn string, silence time.Duration) (*CatchUpMsr, error) {
	cm := &CatchUpMsr{
		active:   make(map[uint64]*catchUpEpisode),
		lastResp: make(map[uint64]int64),
		silence:  silence,
		buff:     &bytes.Buffer{},
	}

	fd, err := createMeasurementFile(fn)
	if err != nil {
		return nil, fmt.Errorf("could not create catch up measurement file: %w", err)
	}
	cm.file = fd

	return cm, nil
}

// NOTE (Gus): per-follower start; no-op if already active for id (guards against repeated
// Start calls while probe/reject cycles continue for the same still-lagging follower), or
// if a full episode was already recorded. targetIndex snapshots the leader's last index at
// this instant, which is the backlog the replication phase later waits on. followerStartIndex
// snapshots the index this catch-up resumes from, i.e. the follower's last acknowledged one
// (pr.Match, which the leader rewinds Next to when it handles the rejection) — a lagging but
// not down follower already holds some entries, so the real backlog it must replicate is
// targetIndex minus this, not targetIndex itself.
func (cm *CatchUpMsr) Start(id uint64, targetIndex uint64, followerStartIndex uint64) {
	if cm.disarmed {
		return
	}
	if _, ok := cm.active[id]; ok {
		return
	}
	cm.active[id] = &catchUpEpisode{
		startNs:            time.Now().UnixNano(),
		targetIndex:        targetIndex,
		followerStartIndex: followerStartIndex,
	}
}

// NOTE (Gus): discards an in-flight measurement for id without recording output — used
// when id stops being quorum-critical for a reason other than id itself catching up (e.g.
// a different follower's ack restored quorum). Once the recovery phase was recorded the
// episode is left alone: its remaining phase tracks pure replication progress, which no
// longer depends on id being quorum-critical.
func (cm *CatchUpMsr) Cancel(id uint64) {
	ep, ok := cm.active[id]
	if !ok || ep.recoveryDone {
		return
	}
	delete(cm.active, id)
}

// NOTE (Gus): reports whether a measurement is in flight for follower id.
func (cm *CatchUpMsr) IsActive(id uint64) bool {
	_, ok := cm.active[id]
	return ok
}

// NOTE (Gus): reports whether the in-flight episode of follower id already cleared the
// peer-silence gate, and is therefore going to produce output.
func (cm *CatchUpMsr) IsPromoted(id uint64) bool {
	ep, ok := cm.active[id]
	return ok && ep.promoted
}

// NOTE (Gus): reports whether a complete episode was already recorded. The measurement is
// single-shot: the experiment injects exactly one failure per run, and everything the
// leader observes afterwards is steady-state replication noise.
func (cm *CatchUpMsr) IsDisarmed() bool {
	return cm.disarmed
}

// NOTE (Gus): reports whether any episode is in flight at all. Lets the leader skip the
// allocation ActiveFollowers() makes, on a path it walks for every response it steps.
func (cm *CatchUpMsr) HasActive() bool {
	return len(cm.active) > 0
}

// NOTE (Gus): snapshot of follower IDs currently being tracked, used to reassess
// criticality after every progress update.
func (cm *CatchUpMsr) ActiveFollowers() []uint64 {
	ids := make([]uint64, 0, len(cm.active))
	for id := range cm.active {
		ids = append(ids, id)
	}
	return ids
}

// NOTE (Gus): the leader last index snapshotted when id's episode started, and whether an
// episode is in flight at all.
func (cm *CatchUpMsr) Target(id uint64) (uint64, bool) {
	ep, ok := cm.active[id]
	if !ok {
		return 0, false
	}
	return ep.targetIndex, true
}

// NOTE (Gus): the follower's own last index snapshotted when id's episode started, and
// whether an episode is in flight at all. See the Start doc comment for why this matters.
func (cm *CatchUpMsr) FollowerStartIndex(id uint64) (uint64, bool) {
	ep, ok := cm.active[id]
	if !ok {
		return 0, false
	}
	return ep.followerStartIndex, true
}

// NOTE (Gus): records that voter id was heard from just now. Feeds the peer-silence gate,
// which is the only signal able to tell a genuine failure apart from a follower that is
// merely lagging — the quorum math alone cannot, since with one chronically delayed voter
// every other voter is momentarily load-bearing at all times.
func (cm *CatchUpMsr) MarkResponse(id uint64) {
	cm.lastResp[id] = time.Now().UnixNano()
}

// NOTE (Gus): last instant voter id was heard from, false if never.
func (cm *CatchUpMsr) LastResponse(id uint64) (int64, bool) {
	ts, ok := cm.lastResp[id]
	return ts, ok
}

func (cm *CatchUpMsr) SilenceThreshold() time.Duration {
	return cm.silence
}

// NOTE (Gus): marks id's in-flight episode as worth recording, peerLastRespNs being the
// last time the silent voter was heard from. The episode must have started after that
// instant: an older episode belongs to the healthy window that preceded the failure, and
// recording it would both backdate the start and burn the single-shot measurement.
func (cm *CatchUpMsr) Promote(id uint64, peerLastRespNs int64) {
	ep, ok := cm.active[id]
	if !ok || ep.promoted || ep.startNs < peerLastRespNs {
		return
	}
	ep.promoted = true
}

// NOTE (Gus): closes the recovery phase — the follower is quorum-critical no more and
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

// NOTE (Gus): closes the replication phase and the episode with it, disarming any further
// measurement. See EndRecovery on endNs.
func (cm *CatchUpMsr) EndReplication(id uint64, endNs int64) {
	cm.endReplication(id, endNs, nil)
}

func (cm *CatchUpMsr) EndReplicationDebug(id uint64, endNs int64, info CatchUpDebugInfo) {
	cm.endReplication(id, endNs, &info)
}

func (cm *CatchUpMsr) endRecovery(id uint64, endNs int64, info *CatchUpDebugInfo) {
	ep, ok := cm.active[id]
	if !ok || ep.recoveryDone {
		return
	}
	// NOTE (Gus): an episode that never cleared the peer-silence gate is dropped
	// silently, freeing id to open a fresh one on its next quorum-critical rejection.
	if !ep.promoted || cm.disarmed {
		delete(cm.active, id)
		return
	}

	cm.record(catchupPhaseRecovery, id, ep, endNs, info)
	ep.recoveryDone = true
}

func (cm *CatchUpMsr) endReplication(id uint64, endNs int64, info *CatchUpDebugInfo) {
	ep, ok := cm.active[id]
	if !ok {
		return
	}
	if !ep.promoted || cm.disarmed {
		delete(cm.active, id)
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
	cm.disarmed = true
}

func (cm *CatchUpMsr) record(phase string, id uint64, ep *catchUpEpisode, endNs int64, info *CatchUpDebugInfo) {
	dur := endNs - ep.startNs

	var err error
	if info == nil {
		_, err = fmt.Fprintf(cm.buff, catchupMeasurementFmt, phase, id, ep.startNs, dur)
	} else {
		_, err = fmt.Fprintf(cm.buff, catchupMeasurementDebugFmt, phase, id, ep.startNs, dur, formatCatchUpDebugInfo(*info))
	}
	if err != nil {
		log.Fatalln("failed recording", phase, "duration, err:", err)
	}

	// NOTE (Gus): a run yields two lines at most, so writing through costs nothing and
	// keeps the measurement from being lost when the leader is killed without a clean
	// etcd shutdown.
	cm.Flush()
}

func formatCatchUpDebugInfo(info CatchUpDebugInfo) string {
	return fmt.Sprintf("[logEntries:%d, target:%d, logleader:{firstIndex:%d, lastIndex:%d, committed:%d, applied:%d}, follower:{id:%d, match:%d, next:%d}]",
		info.LogEntries,
		info.TargetIndex,
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
