package experiment

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	catchupMeasurementFmt      = "%d:%d:%d\n"
	catchupMeasurementDebugFmt = "%d:%d:%d %s\n"
)

type CatchUpDebugInfo struct {
	FollowerID       uint64
	LogEntries       uint64
	LeaderFirstIndex uint64
	LeaderLastIndex  uint64
	LeaderCommitted  uint64
	LeaderApplied    uint64
	FollowerMatch    uint64
	FollowerNext     uint64
}

type CatchUpMsr struct {
	active map[uint64]int64

	buff *bytes.Buffer
	file *os.File
}

func NewCatchUpMsr(fn string) (*CatchUpMsr, error) {
	cm := &CatchUpMsr{
		active: make(map[uint64]int64),
		buff:   &bytes.Buffer{},
	}

	fd, err := createMeasurementFile(fn)
	if err != nil {
		return nil, fmt.Errorf("could not create catch up measurement file: %w", err)
	}
	cm.file = fd

	return cm, nil
}

// NOTE (Gus): per-follower start; no-op if already active for id (guards
// against repeated Start calls while probe/reject cycles continue for the
// same still-lagging follower).
func (cm *CatchUpMsr) Start(id uint64) {
	if _, ok := cm.active[id]; ok {
		return
	}
	cm.active[id] = time.Now().UnixNano()
}

// NOTE (Gus): discards an in-flight measurement for id without recording
// output — used when id stops being quorum-critical for a reason other than
// id itself catching up (e.g. a different follower's ack restored quorum).
func (cm *CatchUpMsr) Cancel(id uint64) {
	delete(cm.active, id)
}

// NOTE (Gus): reports whether a measurement is in flight for follower id.
func (cm *CatchUpMsr) IsActive(id uint64) bool {
	_, ok := cm.active[id]
	return ok
}

// NOTE (Gus): snapshot of follower IDs currently being tracked, used to
// reassess criticality after every progress update.
func (cm *CatchUpMsr) ActiveFollowers() []uint64 {
	ids := make([]uint64, 0, len(cm.active))
	for id := range cm.active {
		ids = append(ids, id)
	}
	return ids
}

func (cm *CatchUpMsr) End(id uint64) {
	cm.end(id, nil)
}

func (cm *CatchUpMsr) EndDebug(id uint64, info CatchUpDebugInfo) {
	cm.end(id, &info)
}

func (cm *CatchUpMsr) end(id uint64, info *CatchUpDebugInfo) {
	start, ok := cm.active[id]
	if !ok {
		return
	}

	now := time.Now().UnixNano()
	dur := now - start

	if info == nil {
		if _, err := fmt.Fprintf(cm.buff, catchupMeasurementFmt, id, start, dur); err != nil {
			log.Fatalln("failed recording duration, err:", err)
		}

	} else {
		if _, err := fmt.Fprintf(cm.buff, catchupMeasurementDebugFmt, id, start, dur, formatCatchUpDebugInfo(*info)); err != nil {
			log.Fatalln("failed recording duration debug, err:", err)
		}
	}
	delete(cm.active, id)
}

func formatCatchUpDebugInfo(info CatchUpDebugInfo) string {
	return fmt.Sprintf("[logEntries:%d, logleader:{firstIndex:%d, lastIndex:%d, committed:%d, applied:%d}, follower:{id:%d, match:%d, next:%d}]",
		info.LogEntries,
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
