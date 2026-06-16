package experiment

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"time"
)

const lagMsgFmt = "node:%s - term:%d - lag_count:%d - ts:%s"

type LagMsr struct {
	tick *time.Ticker

	buff *bytes.Buffer
	file *os.File
}

func NewLagMsr(fn, lagInterval string) (*LagMsr, error) {
	lm := &LagMsr{
		buff: &bytes.Buffer{},
	}

	dur, err := time.ParseDuration(lagInterval)
	if err != nil {
		return nil, fmt.Errorf("could not parse lag interval duration: %w", err)
	}
	lm.tick = time.NewTicker(dur)

	fd, err := createMeasurementFile(fn)
	if err != nil {
		return nil, fmt.Errorf("could not create follower lag file: %w", err)
	}
	lm.file = fd

	return lm, nil
}

func (lm *LagMsr) Run() {
	// TODO: record current lag state at every tick
}

func (lm *LagMsr) Flush() {
	if _, err := lm.buff.WriteTo(lm.file); err != nil {
		log.Fatalln("failed copying from measurement buffer to file, err:", err)
	}

	if err := lm.file.Sync(); err != nil {
		log.Fatalln("failed flushing data to disk, err:", err)
	}
}

func (lm *LagMsr) Close() {
	lm.file.Close()
}

func createMeasurementFile(fn string) (*os.File, error) {
	fd, err := os.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	return fd, nil
}
