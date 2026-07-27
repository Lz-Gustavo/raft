package tracker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/raft/v3/quorum"
)

// newTestTracker builds a 3-voter (ids 1, 2, 3) ProgressTracker with Match set
// per the given map, useful for exercising CommittedWithout without needing a
// full raft instance.
func newTestTracker(match map[uint64]uint64) *ProgressTracker {
	p := MakeProgressTracker(256, 0)
	p.Voters[0] = quorum.MajorityConfig{1: {}, 2: {}, 3: {}}
	for id, m := range match {
		p.Progress[id] = &Progress{Match: m, State: StateReplicate}
	}
	return &p
}

func TestProgressTracker_CommittedWithout(t *testing.T) {
	tests := []struct {
		name     string
		match    map[uint64]uint64
		excluded uint64
		want     uint64
	}{
		{
			// All three voters fully caught up: excluding any one still leaves
			// two others at the last index, so the quorum-critical value is
			// unaffected by the exclusion.
			name:     "all caught up",
			match:    map[uint64]uint64{1: 10, 2: 10, 3: 10},
			excluded: 2,
			want:     10,
		},
		{
			// id 3 is frozen far behind (simulating a dead/partitioned voter).
			// Excluding id 2 (itself caught up) drops quorum down to id 3's
			// stale match, since only {1, 3} remain to form a majority.
			name:     "other voter frozen behind (dead-follower simulation)",
			match:    map[uint64]uint64{1: 10, 2: 10, 3: 3},
			excluded: 2,
			want:     3,
		},
		{
			// Same shape as above but id 3 is merely lagging (alive, still
			// replicating) rather than frozen — the formula doesn't
			// distinguish the two, which is intentional: either way id 2's
			// ack is currently necessary to reach the leader's last index.
			name:     "other voter alive but lagging",
			match:    map[uint64]uint64{1: 10, 2: 10, 3: 7},
			excluded: 2,
			want:     7,
		},
		{
			// The excluded voter (id 3) is the one lagging; the other
			// follower (id 2) is already caught up, so excluding id 3 does
			// not reduce the achievable committed index below the leader's
			// last index — id 3's ack is not quorum-critical.
			name:     "excluded voter is the lagging one, not required for quorum",
			match:    map[uint64]uint64{1: 10, 2: 10, 3: 3},
			excluded: 3,
			want:     10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newTestTracker(tt.match)
			assert.Equal(t, tt.want, p.CommittedWithout(tt.excluded))
		})
	}
}
