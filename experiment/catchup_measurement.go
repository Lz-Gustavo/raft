package experiment

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"
)

const measurementFmt = "%d:%d\n"

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
	if cm.startMsr == 0 {
		return
	}

	now := time.Now().UnixNano()
	dur := now - cm.startMsr

	if _, err := fmt.Fprintf(cm.buff, "%d:%d\n", cm.startMsr, dur); err != nil {
		log.Fatalln("failed recording duration, err:", err)
	}
	cm.startMsr = 0
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
