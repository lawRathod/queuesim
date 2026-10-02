package model

import (
	"math"

	"github.com/law/GoDES/core"
	"github.com/law/GoDES/core/dists"
)

// ProcessorConfig configures a Processor (a "queue → worker → queue" box).
// A Processor consumes tasks from its input buffer, applies a service
// time, and forwards each completed task to its registered output edges
// (through a Network when one is attached).
type ProcessorConfig struct {
	// Name identifies the processor (used in substream keys and
	// diagnostics).
	Name string
	// Budget bounds concurrent in-flight tasks; 0 = unlimited.
	Budget int
	// ServiceMs is the fixed service time in sim ticks. Used when
	// ServiceRate is 0.
	ServiceMs int64
	// ServiceRate, when > 0, samples service time from an exponential
	// distribution with this rate (events per tick): -ln(1-u)/rate.
	// Overrides ServiceMs. Requires ServiceSrc.
	ServiceRate float64
	// ServiceSrc is the deterministic Source for sampled service times.
	// Required when ServiceRate > 0.
	ServiceSrc dists.Source
}

// Processor is a deterministic service-stage component: input buffer,
// bounded in-flight budget, fixed or sampled service time, and output
// edges (each optionally carrying a Network). The topology's pump feeds
// the input buffer and calls tick; completion forwards tasks to outputs.
type Processor struct {
	sim      *core.Simulation
	cfg      ProcessorConfig
	input    []Task          // buffered, awaiting dispatch (delivered by the pump)
	outputs  []*ProcessorOut // output edges in registration order
	inflight int
	served   uint64
	dropped  uint64
}

// ProcessorOut is one output edge from a processor.
type ProcessorOut struct {
	dst *TaskQueue // destination queue
	net *Network   // nil = direct (zero-cost)
}

// NewProcessor creates a Processor bound to sim. Panics on nil sim, empty
// name, negative budget, negative service time, or ServiceRate > 0 with a
// nil ServiceSrc.
func NewProcessor(sim *core.Simulation, cfg ProcessorConfig) *Processor {
	if sim == nil {
		panic("model: NewProcessor requires a non-nil simulation")
	}
	if cfg.Name == "" {
		panic("model: NewProcessor requires a name")
	}
	if cfg.Budget < 0 {
		panic("model: NewProcessor requires a non-negative budget")
	}
	if cfg.ServiceMs < 0 {
		panic("model: NewProcessor requires a non-negative ServiceMs")
	}
	if cfg.ServiceRate > 0 && cfg.ServiceSrc == nil {
		panic("model: NewProcessor requires ServiceSrc when ServiceRate > 0")
	}
	return &Processor{sim: sim, cfg: cfg}
}

// Name returns the processor name.
func (p *Processor) Name() string { return p.cfg.Name }

// InputLen returns the number of tasks currently buffered awaiting
// dispatch.
func (p *Processor) InputLen() int { return len(p.input) }

// InFlight returns the number of tasks currently being serviced.
func (p *Processor) InFlight() int { return p.inflight }

// Served returns the total number of completed (serviced) tasks.
func (p *Processor) Served() uint64 { return p.served }

// Dropped returns the total number of tasks dropped by the processor's
// output networks.
func (p *Processor) Dropped() uint64 { return p.dropped }

// AddOutput registers a downstream queue as an output edge. net may be nil
// for a direct zero-cost edge.
func (p *Processor) AddOutput(dst *TaskQueue, net *Network) {
	if dst == nil {
		panic("model: AddOutput requires a non-nil destination queue")
	}
	p.outputs = append(p.outputs, &ProcessorOut{dst: dst, net: net})
}

// Enqueue buffers delivered tasks into the processor's input (called by
// the topology pump). Deterministic: appended in call order.
func (p *Processor) Enqueue(tasks []Task) {
	p.input = append(p.input, tasks...)
}

// Tick is called by the pump at each boundary: it dispatches up to the
// in-flight budget, scheduling each service completion. Deterministic:
// input FIFO order, no RNG draws beyond the optional service-time sample.
func (p *Processor) Tick(now core.SimTime) {
	for p.cfg.Budget == 0 || p.inflight < p.cfg.Budget {
		if len(p.input) == 0 {
			break
		}
		task := p.input[0]
		p.input = p.input[1:]
		p.inflight++
		svc := p.cfg.ServiceMs
		if p.cfg.ServiceRate > 0 {
			u := p.cfg.ServiceSrc.Float64()
			if u <= 0 {
				u = 1e-9
			}
			svc = int64(math.Round(-math.Log(1-u) / p.cfg.ServiceRate))
			if svc < 0 {
				svc = 0
			}
		}
		p.sim.ScheduleIn(svc, func(*core.Simulation) { p.complete(task) })
	}
}

// complete finalizes one service: decrements in-flight and forwards the
// task through every output edge (through the edge's network when set).
func (p *Processor) complete(task Task) {
	now := p.sim.Now()
	p.inflight--
	p.served++
	for _, out := range p.outputs {
		if out.net != nil {
			if !out.net.Transmit(task, now) {
				p.dropped++ // dropped by the network (lossy link)
			}
			continue
		}
		out.dst.AddAt(task, now)
	}
}
