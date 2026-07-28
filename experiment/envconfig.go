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

	// MeasureFollowerCatchUpPeerSilence is how long another voter must stay silent before
	// an in-flight catch-up is considered a recovery from a real peer failure, and thus
	// worth recording. It must sit above the longest gap a healthy follower can leave
	// (one heartbeat interval plus its round-trip time) and below the recovery duration
	// being measured, otherwise either a routine hiccup or a later steady-state episode
	// is recorded in place of the real one.
	MeasureFollowerCatchUpPeerSilence = "RAFT_MEASURE_FOLLOWER_CATCHUP_PEER_SILENCE"

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

	// EtcdClusterStatusEnabled enables a routine, launched on etcdserver, that periodically outputs
	// cluster information and members status on EtcdClusterStatusFilename.
	EtcdClusterStatusEnabled  = "ETCD_CLUSTER_STATUS_ENABLED"
	EtcdClusterStatusFilename = "ETCD_CLUSTER_STATUS_FILENAME"
)

const (
	defaultFollowerLagFilename       = "/tmp/follower-lag.out"
	defaultFollowerCatchUpFilename   = "/tmp/follower-catchup-time.out"
	defaultEtcdThroughputFilename    = "/tmp/etcd-throughput.out"
	defaultEtcdClusterStatusFilename = "/tmp/etcd-cluster-status.out"

	// NOTE (Gus): one and a half heartbeat intervals of the default 500ms value used on
	// all experiments, which is the longest a healthy follower can stay quiet once its
	// inflights fill up and only heartbeats reach it. Must be retuned along with
	// ETCD_HEARTBEAT_INTERVAL, and stays useful only while shorter than the recovery it
	// is meant to catch.
	defaultFollowerCatchUpPeerSilence = 750 * time.Millisecond
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

	IsEtcdClusterStatusEnabled bool
	EtcdClusterStatusFilename  string
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

		silence := parseEnvDuration(MeasureFollowerCatchUpPeerSilence, defaultFollowerCatchUpPeerSilence)
		Config.CatchUpMsr, err = NewCatchUpMsr(fn, silence)
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

	Config.IsEtcdClusterStatusEnabled = parseEnvBool(EtcdClusterStatusEnabled)
	if Config.IsEtcdClusterStatusEnabled {
		fn, exists := os.LookupEnv(EtcdClusterStatusFilename)
		if !exists {
			fn = defaultEtcdClusterStatusFilename
		}
		Config.EtcdClusterStatusFilename = fn
	}
}

// NOTE (Gus): unlike the other duration configs this one falls back to def instead of
// exiting, since it is an optional tuning knob rather than a required setting.
func parseEnvDuration(env string, def time.Duration) time.Duration {
	raw, exists := os.LookupEnv(env)
	if !exists {
		return def
	}

	val, err := time.ParseDuration(raw)
	if err != nil {
		return def
	}
	return val
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
