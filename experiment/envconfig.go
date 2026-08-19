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

	// MeasureFollowerCatchUpWindow holds how long, as a Go duration string, the recorder
	// keeps accepting episodes once armed. Arming alone was not enough: it decides when to
	// *start* looking, but a single recording slot per follower still went to whichever
	// reject blip arrived first, and under load the first one arrives within milliseconds
	// of the arm instant — far sooner than the margin the harness leaves before the kill.
	// A window drops the single slot instead of trying to aim it: every episode opening in
	// [armAt, armAt+window) is recorded, and picking the one caused by the failure is left
	// to offline analysis, which knows the kill instant exactly and the leader never can.
	// The bound is on when an episode *opens*, never on when it finishes, so a catch-up
	// running well past the window is still measured end to end. Unset means no upper
	// bound, i.e. record every episode from the arm instant to the end of the run.
	MeasureFollowerCatchUpWindow = "RAFT_MEASURE_FOLLOWER_CATCHUP_WINDOW"

	// MeasureFollowerCatchUpSealGrace holds how long, as a Go duration string, the recorder
	// waits past the window's end before writing off whatever is still in flight as abandoned.
	// An episode opening at the very last instant of the window deserves the same time to
	// finish as one opening at its start, which is why the deadline sits past the window rather
	// than at it.
	MeasureFollowerCatchUpSealGrace = "RAFT_MEASURE_FOLLOWER_CATCHUP_SEAL_GRACE"

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

	// EtcdApplyLagEnabled enables a routine, launched on etcdserver, that periodically outputs
	// the leader's committed/applied index gap on EtcdApplyLagFilename. This is the exact
	// quantity EtcdDisableTooManyRequests's check gates on (committed - applied > 5000), and
	// unlike the raft catch-up measurement it is observable even while the shed is rejecting
	// proposals before they ever reach raft.
	EtcdApplyLagEnabled  = "ETCD_MEASURE_APPLY_LAG_ENABLED"
	EtcdApplyLagFilename = "ETCD_MEASURE_APPLY_LAG_FILENAME"

	// EtcdRaftMsgBatchSize overrides raft's MaxSizePerMsg, the byte ceiling on how much log the
	// leader packs into each MsgApp. etcd hardcodes it at 1MiB, which makes the batch size
	// invisible as an experimental variable even though it is the dominant term in how fast a
	// lagging follower is fed during catch-up: the leader can have at most MaxInflightMsgs
	// messages of this size in flight, so the batch sets the bandwidth-delay product the
	// recovery runs at. Unset means the etcd default of 1MiB. 0 means exactly 1 entry per message.
	EtcdRaftMsgBatchSize = "ETCD_RAFT_MSG_BATCH_SIZE"
)

const (
	defaultFollowerLagFilename       = "/tmp/follower-lag.out"
	defaultFollowerCatchUpFilename   = "/tmp/follower-catchup-time.out"
	defaultEtcdThroughputFilename    = "/tmp/etcd-throughput.out"
	defaultEtcdClusterStatusFilename = "/tmp/etcd-cluster-status.out"
	defaultEtcdApplyLagFilename      = "/tmp/etcd-apply-lag.out"

	maxRaftMsgBatchSize = 4 * 1024 * 1024
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

	IsEtcdApplyLagEnabled bool
	EtcdApplyLagFilename  string

	IsRaftMsgBatchSizeSet bool // 0 is a valid size, so it cannot mean unset
	RaftMsgBatchSize      uint64
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

		Config.CatchUpMsr, err = NewCatchUpMsr(fn, catchUpArmNs(), catchUpWindowNs(), catchUpSealGraceNs())
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

	Config.IsEtcdApplyLagEnabled = parseEnvBool(EtcdApplyLagEnabled)
	if Config.IsEtcdApplyLagEnabled {
		fn, exists := os.LookupEnv(EtcdApplyLagFilename)
		if !exists {
			fn = defaultEtcdApplyLagFilename
		}
		Config.EtcdApplyLagFilename = fn
	}

	Config.IsRaftMsgBatchSizeSet, Config.RaftMsgBatchSize = parseRaftMsgBatchSize()
}

// raftMsgBatchSize reports whether the knob is set, and its value.
func parseRaftMsgBatchSize() (bool, uint64) {
	raw, exists := os.LookupEnv(EtcdRaftMsgBatchSize)
	if !exists || raw == "" {
		return false, 0
	}

	val, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || val >= maxRaftMsgBatchSize {
		log.Fatalf(
			"invalid %s=%q: want a plain byte count in [0, %d)",
			EtcdRaftMsgBatchSize, raw, maxRaftMsgBatchSize,
		)
	}
	return true, val
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

// NOTE (Gus): the configured recording window, in ns. Same never-abort contract as
// catchUpArmNs: absent, unparsable, zero or negative all mean 0, i.e. no upper bound —
// the behaviour of a run that arms but never stops recording.
func catchUpWindowNs() int64 {
	return parseEnvDurationNs(MeasureFollowerCatchUpWindow)
}

// NOTE (Gus): the configured grace past the window's end before in-flight episodes are written
// off as abandoned. Same never-abort contract; 0 means "fall back to the window".
func catchUpSealGraceNs() int64 {
	return parseEnvDurationNs(MeasureFollowerCatchUpSealGrace)
}

// parseEnvDurationNs reads a Go duration string into ns, treating absent, unparsable and
// non-positive alike as 0.
func parseEnvDurationNs(env string) int64 {
	raw, exists := os.LookupEnv(env)
	if !exists {
		return 0
	}

	dur, err := time.ParseDuration(raw)
	if err != nil || dur <= 0 || dur > 24*time.Hour {
		return 0
	}
	return int64(dur)
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
