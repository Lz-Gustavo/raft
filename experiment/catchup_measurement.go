package experiment

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	catchupMeasurementFmt      = "%d:%d\n"
	catchupMeasurementDebugFmt = "%d:%d %s\n"
)

type CatchUpDebugInfo struct {
	LogEntries       uint64
	LeaderFirstIndex uint64
	LeaderLastIndex  uint64
	LeaderCommitted  uint64
	LeaderApplied    uint64
	FollowerMatch    uint64
	FollowerNext     uint64
	Message          string
}

type CatchUpMsr struct {
	startMsr int64

	buff *bytes.Buffer
	file *os.File
}

func NewCatchUpMsr(fn string) (*CatchUpMsr, error) {
	cm := &CatchUpMsr{
		buff: &bytes.Buffer{},
	}

	fd, err := createMeasurementFile(fn)
	if err != nil {
		return nil, fmt.Errorf("could not create catch up measurement file: %w", err)
	}
	cm.file = fd

	return cm, nil
}

// Start ...
//
// NOTE: avoids measure if a Start() call was already issued so that responses that are rejected
// by lag, before the catch-up procedure is finished, do not overwrite the initial measurement
func (cm *CatchUpMsr) Start() {
	if cm.startMsr != 0 {
		return
	}
	cm.startMsr = time.Now().UnixNano()
}

func (cm *CatchUpMsr) End() {
	cm.end(nil)
}

func (cm *CatchUpMsr) EndDebug(info CatchUpDebugInfo) {
	cm.end(&info)
}

func (cm *CatchUpMsr) end(info *CatchUpDebugInfo) {
	if cm.startMsr == 0 {
		return
	}

	now := time.Now().UnixNano()
	dur := now - cm.startMsr

	if info == nil {
		if _, err := fmt.Fprintf(cm.buff, catchupMeasurementFmt, cm.startMsr, dur); err != nil {
			log.Fatalln("failed recording duration, err:", err)
		}

	} else {
		if _, err := fmt.Fprintf(cm.buff, catchupMeasurementDebugFmt, cm.startMsr, dur, formatCatchUpDebugInfo(*info)); err != nil {
			log.Fatalln("failed recording duration debug, err:", err)
		}
	}
	cm.startMsr = 0
}

func formatCatchUpDebugInfo(info CatchUpDebugInfo) string {
	return fmt.Sprintf("[logEntries:%d, logleader:{firstIndex:%d, lastIndex:%d, committed:%d, applied:%d}, follower:{match:%d, next:%d}, message:{%s}]",
		info.LogEntries,
		info.LeaderFirstIndex,
		info.LeaderLastIndex,
		info.LeaderCommitted,
		info.LeaderApplied,
		info.FollowerMatch,
		info.FollowerNext,
		info.Message,
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
