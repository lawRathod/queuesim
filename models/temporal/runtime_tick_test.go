package temporal

import (
	"strconv"
	"testing"

	"github.com/lawRathod/queuesim/model/capture"
)

// tickRT builds the model system and returns its runtime with the service
// Build closures materialized and the one-time inflow start skipped, so
// tests can drive rt.tick directly at controlled sim times without burst
// or arrival inflow.
func tickRT(t *testing.T, cfg Config) *runtime {
	t.Helper()
	sys, rt, err := build(cfg)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if err := sys.Build(); err != nil {
		t.Fatalf("sys.Build: %v", err)
	}
	rt.started = true // skip burst/arrival scheduling; drive ticks directly
	return rt
}

// TestTickReschedBypassesTransferQueue pins the reschedule-to-scheduler
// wiring: a due rescheduled task re-enters matching via the add boundary
// (scheduler submit) in the SAME tick — it must not park a hop in the
// history transfer queue (the legacy fallback when Tick is invoked without
// the onResched hook).
func TestTickReschedBypassesTransferQueue(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 42
	cfg.BurstCount = 1
	cfg.Shards = 1
	cfg.MatchingCapacity = 1000
	cfg.Vector = nil
	rt := tickRT(t, cfg)

	now := rt.sim.Now()
	before := rt.stats.AttemptedAdds
	rt.shards[0].RescheduleAt(Task{ID: 7, Attempt: 1, Class: TaskActivity, BornMs: 0, Rescheduled: true}, now.Int64())

	rt.tick(now)

	if got := rt.shards[0].LenResched(); got != 0 {
		t.Fatalf("resched len = %d, want 0 (due task consumed)", got)
	}
	if got := rt.shards[0].LenTransfer(); got != 0 {
		t.Fatalf("transfer len = %d, want 0 (due resched must bypass the transfer queue)", got)
	}
	if got := rt.stats.AttemptedAdds; got != before+1 {
		t.Fatalf("attempted adds = %d, want %d (due resched re-added via the matching boundary)", got, before+1)
	}
}

// TestTickShardRotation pins the deterministic shard visit order: tick k
// starts at shard k%len(shards), wrapping with zero RNG. Observed via the
// transfer_wf capture sequence with one transfer task per shard per tick.
func TestTickShardRotation(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 42
	cfg.BurstCount = 1
	cfg.Shards = 3
	cfg.MatchingCapacity = 1000
	cfg.Vector = nil
	rt := tickRT(t, cfg)
	rec := capture.NewRecorder()
	rt.attachCap(rec)

	transferIDs := func() []string {
		var ids []string
		for _, e := range rec.Events() {
			if e.Kind == "transfer_wf" {
				ids = append(ids, e.TaskID)
			}
		}
		return ids
	}

	for k := 0; k < 4; k++ {
		for i := range rt.shards {
			rt.shards[i].EnqueueTransfer(Task{ID: uint64(k*10 + i), Attempt: 1, Class: TaskWfStart, BornMs: 0})
		}
		base := len(transferIDs())
		rt.sim.Advance(1)
		rt.tick(rt.sim.Now())
		got := transferIDs()[base:]
		if len(got) != len(rt.shards) {
			t.Fatalf("tick %d: transfer events = %d, want %d (one per shard)", k, len(got), len(rt.shards))
		}
		start := k % len(rt.shards)
		for j := range rt.shards {
			want := strconv.Itoa(k*10 + (start+j)%len(rt.shards))
			if got[j] != want {
				t.Fatalf("tick %d: transfer order = %v, want start shard %d (tickCount rotation)", k, got, start)
			}
		}
	}
	if rt.tickCount != 4 {
		t.Fatalf("tickCount = %d, want 4 (one increment per tick)", rt.tickCount)
	}
}
