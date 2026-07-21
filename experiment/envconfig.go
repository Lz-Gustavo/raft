package experiment

import (
	"log"
	"os"
	"strconv"
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

	// MeasureFollowerCatchUpDebugEnabled enables extra fields in catch-up
	// measurements to help inspect log replication during recovery.
	MeasureFollowerCatchUpDebugEnabled = "RAFT_MEASURE_FOLLOWER_CATCHUP_DEBUG_ENABLED"

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

	// MeasureEtcdThroughputEnabled ...
	MeasureEtcdThroughputEnabled  = "ETCD_MEASURE_THR_ENABLED"
	MeasureEtcdThroughputFilename = "ETCD_MEASURE_THR_FILENAME"

	// EtcdDisableTooManyRequests allows the disable of "too many requests" errors that are returned
	// by etcd when the difference between the apply (executed) to commit (decided by Raft) indexes
	// exceeds a 5k limit. The error is intended to avoid over-saturating the database when is already
	// delayed. This config var is implemented to allow an evaluation on how the commit/apply index gap
	// evolves on a scenario with hetegoneous latency values between participants.
	EtcdDisableTooManyRequests = "ETCD_DISABLE_TOO_MANY_REQUESTS"
)

const (
	defaultFollowerLagFilename     = "/tmp/follower-lag.out"
	defaultFollowerCatchUpFilename = "/tmp/follower-catchup-time.out"
	defaultEtcdThroughputFilename  = "/tmp/etcd-throughput.out"
)

var Config = ExpConfig{}

type ExpConfig struct {
	IsMeasureFollowerLagEnabled bool
	LagMsr                      *LagMsr

	IsMeasureFollowerCatchUpEnabled      bool
	CatchUpMsr                           *CatchUpMsr
	IsMeasureFollowerCatchUpDebugEnabled bool

	IsBeelogCatchUpEnabled       bool
	IsNearFollowerCatchUpEnabled bool

	IsLocalFollowerLatencyEnabled bool
	LocalFollowerLatencyDuration  time.Duration

	IsMeasureEtcdThoughputEnabled bool
	ThrMsr                        *ThrMsr

	IsEtcdTooManyRequestsDisabled bool
}

func LoadEnvConfig() {
	var err error

	Config.IsMeasureFollowerLagEnabled = parseEnvBool(MeasureFollowerLagEnabled)
	if Config.IsMeasureFollowerLagEnabled {
		fn, exists := os.LookupEnv(MeasureFollowerLagFilename)
		if !exists {
			fn = defaultFollowerLagFilename
		}

		Config.LagMsr, err = NewLagMsr(fn, os.Getenv(MeasureFollowerLagInterval))
		if err != nil {
			log.Fatalln(err)
		}
	}

	Config.IsMeasureFollowerCatchUpEnabled = parseEnvBool(MeasureFollowerCatchUpEnabled)
	if Config.IsMeasureFollowerCatchUpEnabled {
		fn, exists := os.LookupEnv(MeasureFollowerCatchUpFilename)
		if !exists {
			fn = defaultFollowerCatchUpFilename
		}

		Config.CatchUpMsr, err = NewCatchUpMsr(fn)
		if err != nil {
			log.Fatalln(err)
		}
	}
	Config.IsMeasureFollowerCatchUpDebugEnabled = parseEnvBool(MeasureFollowerCatchUpDebugEnabled)

	Config.IsBeelogCatchUpEnabled = parseEnvBool(BeelogCatchUpEnabled)
	Config.IsNearFollowerCatchUpEnabled = parseEnvBool(NearFollowerCatchUpEnabled)

	Config.IsLocalFollowerLatencyEnabled = parseEnvBool(LocalFollowerLatencyEnabled)
	if Config.IsLocalFollowerLatencyEnabled {
		Config.LocalFollowerLatencyDuration, err = time.ParseDuration(os.Getenv(LocalFollowerLatencyDuration))
		if err != nil {
			log.Fatalln(err)
		}
	}

	Config.IsMeasureEtcdThoughputEnabled = parseEnvBool(MeasureEtcdThroughputEnabled)
	if Config.IsMeasureEtcdThoughputEnabled {
		fn, exists := os.LookupEnv(MeasureEtcdThroughputFilename)
		if !exists {
			fn = defaultEtcdThroughputFilename
		}

		Config.ThrMsr, err = NewThrMsr(fn)
		if err != nil {
			log.Fatalln(err)
		}
	}
	Config.IsEtcdTooManyRequestsDisabled = parseEnvBool(EtcdDisableTooManyRequests)
}

func parseEnvBool(env string) bool {
	raw, exists := os.LookupEnv(env)
	if !exists {
		return false
	}

	val, err := strconv.ParseBool(raw)
	if err != nil {
		return false
	}
	return val
}
