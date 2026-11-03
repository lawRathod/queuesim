package model

import (
	"github.com/lawRathod/queuesim/godes/core"
	"github.com/lawRathod/queuesim/godes/core/dists"
)

// NetworkConfig configures a lossy/delayed link between components.
type NetworkConfig struct {
	// Name identifies the link (diagnostics).
	Name string
	// DelayMs is the fixed one-way propagation delay in sim ticks.
	DelayMs int64
	// DropRate is the probability a transmitted message is lost
	// (0 = no loss). Draws use LinkSrc.
	DropRate float64
	// JitterMs, when > 0, adds a uniform jitter in [0, JitterMs) to the
	// delay (per message).
	JitterMs int64
	// LinkSrc is the deterministic Source used for drop and jitter draws.
	// Required when DropRate > 0 or JitterMs > 0.
	LinkSrc dists.Source
}

// Network is a deterministic lossy/delayed message link. Transmit either
// drops the message (with probability DropRate), or schedules its delivery
// at now + DelayMs (+ jitter). Delivery is drained by the topology pump.
type Network struct {
	sim     *core.Simulation
	cfg     NetworkConfig
	pending []pendingMsg // in-flight messages awaiting delivery
	dropped uint64
	sent    uint64
}

type pendingMsg struct {
	task Task
	due  core.SimTime
}

// NewNetwork creates a Network bound to sim. Panics on nil sim, negative
// DelayMs/JitterMs, DropRate outside [0,1], or a required-but-nil LinkSrc.
func NewNetwork(sim *core.Simulation, cfg NetworkConfig) *Network {
	if sim == nil {
		panic("model: NewNetwork requires a non-nil simulation")
	}
	if cfg.DelayMs < 0 || cfg.JitterMs < 0 {
		panic("model: NewNetwork requires non-negative delays")
	}
	if cfg.DropRate < 0 || cfg.DropRate > 1 {
		panic("model: NewNetwork requires DropRate in [0,1]")
	}
	if (cfg.DropRate > 0 || cfg.JitterMs > 0) && cfg.LinkSrc == nil {
		panic("model: NewNetwork requires LinkSrc when DropRate > 0 or JitterMs > 0")
	}
	return &Network{sim: sim, cfg: cfg}
}

// Transmit sends task over the link. Returns true if accepted for
// delivery (scheduled), false if dropped. Deterministic: drop decided
// first; delay/jitter computed from the configured Source.
func (n *Network) Transmit(task Task, now core.SimTime) bool {
	n.sent++
	if n.cfg.DropRate > 0 && n.cfg.LinkSrc.Float64() < n.cfg.DropRate {
		n.dropped++
		return false
	}
	delay := n.cfg.DelayMs
	if n.cfg.JitterMs > 0 {
		delay += int64(n.cfg.LinkSrc.Float64() * float64(n.cfg.JitterMs))
	}
	n.pending = append(n.pending, pendingMsg{task: task, due: now.Add(delay)})
	return true
}

// DrainToTick delivers every message whose due time is <= target, in
// transmission (FIFO) order. Returns the delivered tasks.
func (n *Network) DrainToTick(target core.SimTime) []Task {
	if len(n.pending) == 0 {
		return nil
	}
	var delivered []Task
	var remaining []pendingMsg
	for _, m := range n.pending {
		if m.due <= target {
			delivered = append(delivered, m.task)
		} else {
			remaining = append(remaining, m)
		}
	}
	n.pending = remaining
	return delivered
}

// Pending returns the number of messages currently in flight on the link.
func (n *Network) Pending() int { return len(n.pending) }

// Name returns the network's configured name.
func (n *Network) Name() string { return n.cfg.Name }

// Dropped returns the total number of dropped messages.
func (n *Network) Dropped() uint64 { return n.dropped }

// Sent returns the total number of transmit attempts.
func (n *Network) Sent() uint64 { return n.sent }
