package experiment

import (
	"log"
	"os"
	"time"
)

const (
	// MeasureFollowerLagEnabled triggers the periodically measurement of follower
	// replication lag at every MeasureFollowerLagInterval duration. The measurements
	// are first stored in-memory, and later persisted on MeasureFollowerLagFilename file.
	MeasureFollowerLagEnabled  = "RAFT_MEASURE_FOLLOWER_LAG_ENABLED"
	MeasureFollowerLagInterval = "RAFT_MEASURE_FOLLOWER_LAG_INTERVAL"
	MeasureFollowerLagFilename = "RAFT_MEASURE_FOLLOWER_LAG_FILENAME"

	// MeasureFollowerCatchUpEnabled triggers the measurement of the time taken to
	// restore a delayed follower after the leader identifies a rejection and is able
	// to send new entries. The measurements are taken on milliseconds, and are first
	// stored in-memory to be later persisted on MeasureFollowerCatchUpFilename file.
	MeasureFollowerCatchUpEnabled  = "RAFT_MEASURE_FOLLOWER_CATCHUP_ENABLED"
	MeasureFollowerCatchUpFilename = "RAFT_MEASURE_FOLLOWER_CATCHUP_FILENAME"

	// BeelogCatchUpEnabled enables the usage of a compacted in-memory state to serve delayed
	// replicas during catch-up phase.
	BeelogCatchUpEnabled = "RAFT_BEELOG_CATCHUP_ENABLED"

	// NearFollowerCatchUpEnabled allows followers to retrieve the recovery state from
	// near located followers instead of relying on the leader. BeelogCatchUpEnabled flag
	// must also be enabled.
	NearFollowerCatchUpEnabled = "RAFT_NEAR_FOLLOWER_ENABLED"

	// LocalFollowerLatencyEnabled enables an artificial latency increase on every follower
	// response in the form of a Sleep() call for LocalFollowerLatencyDuration, in order to
	// simulate a follower delay on local evaluations. Must not be enabled when evaluating
	// on a distributed environment.
	LocalFollowerLatencyEnabled  = "RAFT_LOCAL_FOLLOWER_LATENCY_ENABLED"
	LocalFollowerLatencyDuration = "RAFT_LOCAL_FOLLOWER_LATENCY_DURATION"
)

const (
	defaultFollowerLagFilename     = "/tmp/follower-lag.out"
	defaultFollowerCatchUpFilename = "/tmp/follower-catchup-time.out"
)

var Config = ExpConfig{}

type ExpConfig struct {
	IsMeasureFollowerLagEnabled bool
	LagMsr                      *LagMsr

	IsMeasureFollowerCatchUpEnabled bool
	CatchUpMsr                      *CatchUpMsr

	IsBeelogCatchUpEnabled       bool
	IsNearFollowerCatchUpEnabled bool

	IsLocalFollowerLatencyEnabled bool
	LocalFollowerLatencyDuration  time.Duration
}

func LoadEnvConfig() {
	_, Config.IsMeasureFollowerLagEnabled = os.LookupEnv(MeasureFollowerLagEnabled)
	if Config.IsMeasureFollowerLagEnabled {
		fn, exists := os.LookupEnv(MeasureFollowerLagFilename)
		if !exists {
			fn = defaultFollowerLagFilename
		}

		lm, err := NewLagMsr(fn, os.Getenv(MeasureFollowerLagInterval))
		if err != nil {
			log.Fatalln(err)
		}
		Config.LagMsr = lm
	}

	_, Config.IsMeasureFollowerCatchUpEnabled = os.LookupEnv(MeasureFollowerCatchUpEnabled)
	if Config.IsMeasureFollowerCatchUpEnabled {
		fn, exists := os.LookupEnv(MeasureFollowerCatchUpFilename)
		if !exists {
			fn = defaultFollowerCatchUpFilename
		}

		cm, err := NewCatchUpMsr(fn)
		if err != nil {
			log.Fatalln(err)
		}
		Config.CatchUpMsr = cm
	}

	_, Config.IsBeelogCatchUpEnabled = os.LookupEnv(BeelogCatchUpEnabled)
	_, Config.IsNearFollowerCatchUpEnabled = os.LookupEnv(NearFollowerCatchUpEnabled)

	// NOTE (Gus): maybe refac to getenv -> parse pool instead to avoid missuse?
	_, Config.IsLocalFollowerLatencyEnabled = os.LookupEnv(LocalFollowerLatencyEnabled)
	if Config.IsLocalFollowerLatencyEnabled {
		dur, err := time.ParseDuration(os.Getenv(LocalFollowerLatencyDuration))
		if err != nil {
			log.Fatalln(err)
		}
		Config.LocalFollowerLatencyDuration = dur
	}
}
