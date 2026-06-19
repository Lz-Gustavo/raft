package experiment

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"
)

type ThrMsr struct {
	tick  *time.Ticker
	count *atomic.Uint32

	buff *bytes.Buffer
	file *os.File
}

func NewThrMsr(fn string) (*ThrMsr, error) {
	tm := &ThrMsr{
		tick:  time.NewTicker(time.Second),
		buff:  &bytes.Buffer{},
		count: &atomic.Uint32{},
	}

	fd, err := createMeasurementFile(fn)
	if err != nil {
		return nil, fmt.Errorf("could not create throughput file: %w", err)
	}
	tm.file = fd
	return tm, nil
}

func (tm *ThrMsr) Count() {
	tm.count.Add(1)
}

func (tm *ThrMsr) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-tm.tick.C:
			t := tm.count.Swap(0)
			_, err := fmt.Fprintf(tm.buff, "%d\n", t)
			if err != nil {
				log.Fatalln("failed writing throughput measurement, err:", err.Error())
				return
			}
		}
	}
}

func (tm *ThrMsr) Flush() {
	if _, err := tm.buff.WriteTo(tm.file); err != nil {
		log.Fatalln("failed copying from measurement buffer to file, err:", err)
	}

	if err := tm.file.Sync(); err != nil {
		log.Fatalln("failed flushing data to disk, err:", err)
	}
}

func (tm *ThrMsr) Close() {
	tm.file.Close()
}
