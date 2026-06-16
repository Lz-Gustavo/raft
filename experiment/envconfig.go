package experiment

import (
	"log"
	"os"
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
)

const (
	defaultFollowerLagFilename     = "/tmp/follower-lag.out"
	defaultFollowerCatchUpFilename = "/tmp/follower-catchup-time.out"
)

var Config = ExpConfig{}

type ExpConfig struct {
	MeasureFollowerLagEnabled bool
	LagMsr                    *LagMsr

	MeasureFollowerCatchUpEnabled bool
	CatchUpMsr                    *CatchUpMsr

	BeelogCatchUpEnabled       bool
	NearFollowerCatchUpEnabled bool
}

func LoadEnvConfig() {
	_, Config.MeasureFollowerLagEnabled = os.LookupEnv(MeasureFollowerLagEnabled)
	if Config.MeasureFollowerLagEnabled {
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

	_, Config.MeasureFollowerCatchUpEnabled = os.LookupEnv(MeasureFollowerCatchUpEnabled)
	if Config.MeasureFollowerCatchUpEnabled {
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

	_, Config.BeelogCatchUpEnabled = os.LookupEnv(BeelogCatchUpEnabled)
	_, Config.NearFollowerCatchUpEnabled = os.LookupEnv(NearFollowerCatchUpEnabled)
}
