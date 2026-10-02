package model

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/law/GoDES/core"
)

// demoSystem builds the WP1 reference model: a producer service (one out
// queue) and a consumer service (in queue → Processor → done out queue),
// connected producer.out → consumer.in. Returns the System plus the
// author-side Service values.
func demoSystem(t *testing.T, cfg SystemConfig) (*System, Service, Service) {
	t.Helper()
	producer := Service{Name: "producer", Kind: "src", Ports: []Port{{Name: "out"}}}
	producer.Build = func(b *TopologyBuilder) error {
		b.AddQueue("out", 0)
		b.ExportOut("out")
		return nil
	}
	consumer := Service{Name: "consumer", Kind: "sink", Ports: []Port{{Name: "in", In: true}, {Name: "done"}}}
	consumer.Build = func(b *TopologyBuilder) error {
		b.AddQueue("in", 100)
		b.AddQueue("done", 100)
		b.AddProcessor(ProcessorConfig{Name: "work", Budget: 2, ServiceMs: 2})
		b.Feed(b.Queue("in"), b.Processor("work"))
		b.Connect(b.Processor("work"), b.Queue("done"), nil)
		b.ExportIn("in")
		b.ExportOut("done")
		return nil
	}
	sys := NewSystem("demo", cfg)
	sys.AddService(producer).AddService(consumer)
	sys.Connect("producer", "out", "consumer", "in", NetworkConfig{})
	return sys, producer, consumer
}

// runDemo builds + seeds + runs a demo system, recording a per-tick depth
// trace through the read-only tick-hook escape hatch.
func runDemo(t *testing.T, cfg SystemConfig, seedTasks int) (digest string, trace []int64, served uint64, simEnd int64) {
	t.Helper()
	sys, _, _ := demoSystem(t, cfg)
	sys.AddTickHook(func(now core.SimTime) {
		trace = append(trace, int64(sys.Queue("consumer", "in").Len()))
	})
	if err := sys.Build(); err != nil {
		t.Fatal(err)
	}
	out := sys.Queue("producer", "out")
	for i := 0; i < seedTasks; i++ {
		out.Add(Task{ID: uint64(i), Attempt: 1})
	}
	res := sys.Run()
	served = sys.Topology().Child("consumer").Processor("work").Served()
	return res.Digest, trace, served, res.SimEndMs
}

// TestSystemDeterminismN5 is the WP1 determinism gate: same model + same
// seed → bit-identical result (monitor ON), N=5.
func TestSystemDeterminismN5(t *testing.T) {
	cfg := SystemConfig{Seed: 42, PumpMs: 1, SimEndMs: 200}
	var firstDigest string
	var firstTrace string
	for i := 0; i < 5; i++ {
		digest, trace, served, simEnd := runDemo(t, cfg, 10)
		if i == 0 {
			firstDigest = digest
			firstTrace = fmt.Sprintf("%v", trace)
		}
		if digest != firstDigest {
			t.Fatalf("run %d: digest mismatch\n%q\n%q", i, digest, firstDigest)
		}
		if fmt.Sprintf("%v", trace) != firstTrace {
			t.Fatalf("run %d: trace mismatch", i)
		}
		if served != 10 {
			t.Fatalf("run %d: served=%d want 10", i, served)
		}
		if simEnd != 200 {
			t.Fatalf("run %d: simEnd=%d want 200", i, simEnd)
		}
	}
	t.Logf("determinism digest (run 0): %q", firstDigest)
	t.Logf("sha256(digest)=%x", sha256.Sum256([]byte(firstDigest)))
}

// TestSystemMonitorOffEqualOn is the monitor-neutrality gate: the
// simulation trace with monitoring ON is bit-identical to monitoring OFF.
func TestSystemMonitorOffEqualOn(t *testing.T) {
	on := SystemConfig{Seed: 7, PumpMs: 1, SimEndMs: 150}
	off := SystemConfig{Seed: 7, PumpMs: 1, SimEndMs: 150, DisableMonitor: true}
	_, _, servedOn, endOn := runDemo(t, on, 8)
	_, _, servedOff, endOff := runDemo(t, off, 8)
	digestOn, traceOn, _, _ := runDemo(t, on, 8)
	digestOff, traceOff, _, _ := runDemo(t, off, 8)

	if servedOn != servedOff || endOn != endOff {
		t.Fatalf("final sim state differs: served %d/%d end %d/%d", servedOn, servedOff, endOn, endOff)
	}
	if fmt.Sprintf("%v", traceOn) != fmt.Sprintf("%v", traceOff) {
		t.Fatal("depth sequence differs between monitor on/off")
	}
	// Monitoring off leaves the digest empty; on produces a deterministic
	// digest. The simulation behavior is what must be bit-identical.
	if digestOn == "" || digestOff != "" {
		t.Fatalf("unexpected digests: on=%q off=%q", digestOn, digestOff)
	}
}

// TestSystemWithDisabledMonitorNoMetrics asserts the off-path produces no
// monitor metrics at all.
func TestSystemWithDisabledMonitorNoMetrics(t *testing.T) {
	sys, _, _ := demoSystem(t, SystemConfig{Seed: 3, PumpMs: 1, SimEndMs: 50, DisableMonitor: true})
	if err := sys.Build(); err != nil {
		t.Fatal(err)
	}
	if sys.Monitor() != nil {
		t.Fatal("expected nil monitor when disabled")
	}
}

// TestSystemLifecycleGuards asserts Build-twice errors and Run-twice panics
// (the determinism contract: a system produces exactly one trace).
func TestSystemLifecycleGuards(t *testing.T) {
	sys, _, _ := demoSystem(t, SystemConfig{Seed: 3, PumpMs: 1, SimEndMs: 50})
	if err := sys.Build(); err != nil {
		t.Fatal(err)
	}
	if err := sys.Build(); err == nil || !strings.Contains(err.Error(), "already built") {
		t.Fatalf("second Build should error, got %v", err)
	}
	out := sys.Queue("producer", "out")
	out.Add(Task{ID: 1, Attempt: 1})
	sys.Run()
	defer func() {
		if recover() == nil {
			t.Fatal("second Run should panic")
		}
	}()
	sys.Run()
}

// TestSystemWriteDotDeterminism is the schematic gate: two renders are
// byte-identical and every node comes from the real service graph.
func TestSystemWriteDotDeterminism(t *testing.T) {
	render := func() string {
		sys, _, _ := demoSystem(t, SystemConfig{Seed: 11, PumpMs: 1, SimEndMs: 50})
		if err := sys.Build(); err != nil {
			t.Fatal(err)
		}
		out := sys.Queue("producer", "out")
		out.Add(Task{ID: 1, Attempt: 1})
		sys.Run()
		var buf bytes.Buffer
		if err := sys.WriteDot(&buf); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}
	a, b := render(), render()
	if a != b {
		t.Fatal("WriteDot not byte-identical across runs")
	}
	// Real entities: service clusters + real queues/ports + real processor.
	if !strings.Contains(a, "cluster_producer") || !strings.Contains(a, "cluster_consumer") {
		t.Fatal("DOT missing service clusters:\n" + a)
	}
	if !strings.Contains(a, "q_producer_out") || !strings.Contains(a, "q_consumer_in") || !strings.Contains(a, "q_consumer_done") {
		t.Fatal("DOT missing real service queues/ports:\n" + a)
	}
	if !strings.Contains(a, "p_consumer_work") {
		t.Fatal("DOT missing real processor:\n" + a)
	}
	if !strings.Contains(a, "q_producer_out -> q_consumer_in [color=blue];") {
		t.Fatal("DOT missing real connection edge:\n" + a)
	}
	t.Logf("DOT sample:\n%s", a)
}

// TestSystemWithSimEscapeHatch exercises WithSim: the System adopts an
// externally-created simulation (the author may pre-schedule from their own
// sim reference) and runs identically.
func TestSystemWithSimEscapeHatch(t *testing.T) {
	sim, err := core.NewSimulation(99, core.FIFOPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	producer := Service{Name: "producer", Kind: "src", Ports: []Port{{Name: "out"}}}
	producer.Build = func(b *TopologyBuilder) error {
		b.AddQueue("out", 0)
		b.ExportOut("out")
		// Pre-schedule arrivals from the author's own sim reference.
		for i := 0; i < 5; i++ {
			sim.ScheduleIn(int64(i*10), func(s *core.Simulation) {
				b.Queue("out").Add(Task{ID: uint64(i), Attempt: 1})
			})
		}
		return nil
	}
	consumer := Service{Name: "consumer", Kind: "sink", Ports: []Port{{Name: "in", In: true}}}
	consumer.Build = func(b *TopologyBuilder) error {
		b.AddQueue("in", 100)
		b.AddProcessor(ProcessorConfig{Name: "work", Budget: 2, ServiceMs: 2})
		b.Feed(b.Queue("in"), b.Processor("work"))
		b.ExportIn("in")
		return nil
	}
	sys := NewSystem("esc", SystemConfig{PumpMs: 1, SimEndMs: 100})
	sys.AddService(producer).AddService(consumer)
	sys.Connect("producer", "out", "consumer", "in", NetworkConfig{})
	sys.WithSim(sim)
	res := sys.Run()
	if sys.Sim() != sim {
		t.Fatal("WithSim did not adopt the provided simulation")
	}
	if got := sys.Topology().Child("consumer").Processor("work").Served(); got != 5 {
		t.Fatalf("WithSim run served=%d want 5", got)
	}
	if len(res.Metrics.Gauges) == 0 {
		t.Fatal("expected automatic queue-depth gauges in metrics")
	}
	if res.Digest == "" {
		t.Fatal("expected a metrics digest")
	}
	t.Logf("WithSim digest: %q", res.Digest)
}

// TestSystemWorkerService exercises a Worker-fleet service inside a System:
// the pump parks the child's workers and the automatic throughput counter
// reports completed executions.
func TestSystemWorkerService(t *testing.T) {
	producer := Service{Name: "producer", Kind: "src", Ports: []Port{{Name: "out"}}}
	producer.Build = func(b *TopologyBuilder) error {
		b.AddQueue("out", 0)
		b.ExportOut("out")
		return nil
	}
	consumer := Service{Name: "consumer", Kind: "worker-sink", Ports: []Port{{Name: "in", In: true}}}
	consumer.Build = func(b *TopologyBuilder) error {
		b.AddQueue("in", 100)
		b.AddWorker(WorkerConfig{Name: "workers", Queue: b.Queue("in"), ServiceMs: 2, Count: 2, LongPollMs: 20})
		b.ExportIn("in")
		return nil
	}
	sys := NewSystem("worker", SystemConfig{Seed: 5, PumpMs: 1, SimEndMs: 200})
	sys.AddService(producer).AddService(consumer)
	sys.Connect("producer", "out", "consumer", "in", NetworkConfig{})
	if err := sys.Build(); err != nil {
		t.Fatal(err)
	}
	out := sys.Queue("producer", "out")
	for i := 0; i < 10; i++ {
		out.Add(Task{ID: uint64(i), Attempt: 1})
	}
	res := sys.Run()
	if got := sys.Worker("consumer", "workers").Completed(); got != 10 {
		t.Fatalf("worker completed=%d want 10", got)
	}
	// The throughput counter must be present in the metrics.
	found := false
	for _, c := range res.Metrics.Counters {
		if c.Name == "consumer.workers.completed" && c.Value == 10 {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing throughput counter in metrics: %+v", res.Metrics.Counters)
	}
}

// TestSystemRunFinalSampleReflectsPostHorizonEvents is the WP3 final-sample
// gate: the sim runs to queue exhaustion (events past the horizon still
// fire), and a counter changed only by a post-horizon event must be
// reflected in the finalized metrics. The System takes one final monitor
// sample after the pump stops so counters/histograms cover the whole trace.
func TestSystemRunFinalSampleReflectsPostHorizonEvents(t *testing.T) {
	sys, _, _ := demoSystem(t, SystemConfig{Seed: 1, PumpMs: 1, SimEndMs: 50})
	if err := sys.Build(); err != nil {
		t.Fatal(err)
	}
	var late uint64
	sys.Monitor().AddCounter("late.events", func() uint64 { return late })
	// The last pump tick is at SimEndMs; this event fires after it, so the
	// per-tick samples never see it — only a post-run final sample can.
	sys.Sim().ScheduleAt(55, func(*core.Simulation) { late = 1 })
	res := sys.Run()
	got, ok := res.Metrics.Counter("late.events")
	if !ok {
		t.Fatalf("missing counter late.events in metrics: %+v", res.Metrics.Counters)
	}
	if got.Value != 1 {
		t.Fatalf("late counter = %d, want 1 (final sample must read post-horizon state)", got.Value)
	}
}

// TestSystemConnectValidation pins the Connect-time guards: invalid network
// config and named-but-empty networks panic.
func TestSystemConnectValidation(t *testing.T) {
	assertPanicMsg := func(name string, fn func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: expected panic", name)
			}
		}()
		fn()
	}
	sys := NewSystem("conn", SystemConfig{Seed: 1})
	assertPanicMsg("negative delay", func() {
		sys.Connect("a", "out", "b", "in", NetworkConfig{DelayMs: -1})
	})
	assertPanicMsg("drop > 1", func() {
		sys.Connect("a", "out", "b", "in", NetworkConfig{DropRate: 2})
	})
	assertPanicMsg("named empty network", func() {
		sys.Connect("a", "out", "b", "in", NetworkConfig{Name: "x"})
	})
}