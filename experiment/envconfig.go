package experiment

import (
	"log"
	"math"
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

	// MeasureFollowerCatchUpArmAt holds the absolute Unix instant, in SECONDS, before which
	// catch-up episodes are ignored. The leader cannot tell an episode provoked by load
	// onset from one provoked by the injected failure; the experiment harness can, since it
	// decides when to kill a follower, so it hands the leader that instant here and every
	// episode opening earlier is discarded as noise. Seconds because `date +%s` is then
	// enough to produce it and the value stays readable — the window this has to land in is
	// seconds wide, so sub-second precision would buy nothing. Unset means armed from the
	// start, i.e. measure every episode, which is what runs not injecting a failure want.
	MeasureFollowerCatchUpArmAt = "RAFT_MEASURE_FOLLOWER_CATCHUP_ARM_AT"

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

		Config.CatchUpMsr, err = NewCatchUpMsr(fn, catchUpArmNs())
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

// NOTE (Gus): the configured arm instant, converted to the ns the measurement compares
// against time.Now().UnixNano(). Like every other tuning knob here it must never abort the
// server: absent, unparsable, negative, or large enough to overflow the conversion all mean
// 0, i.e. armed from the start — a measurement parameter is not worth failing a run over,
// and the fallback is the behaviour the recorder had before arming existed.
func catchUpArmNs() int64 {
	sec := parseEnvInt64(MeasureFollowerCatchUpArmAt)
	if sec <= 0 || sec > math.MaxInt64/int64(time.Second) {
		return 0
	}
	return sec * int64(time.Second)
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

func parseEnvInt64(env string) int64 {
	raw, exists := os.LookupEnv(env)
	if !exists {
		return 0
	}

	val, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	return val
}
