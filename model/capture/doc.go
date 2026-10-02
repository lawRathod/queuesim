// Package capture provides a model-agnostic, deterministic, observational
// mechanism recorder for GoDES models. Models record transitions between
// their OWN component names at real chokepoints
//
//	if rt.cap != nil {
//	    rt.cap.Record(from, to, kind, tsMs, capture.WithTask(id, attempt, class))
//	}
//
// an aggregator (Aggregate) folds the recorded JSONL events into a
// count/timing multigraph, and a DOT emitter (Graph.WriteDOT) renders the
// mechanism map ONLY from observed behavior — what actually traversed the
// model. Capture is a first-class library capability, not a temporal
// special: any model that records transitions gets the derived map.
//
// Determinism and neutrality contract:
//
//   - Capture is DEFAULT-OFF and byte-neutral. When a model's capture flag
//     is false the recorder is nil and Record is never called — the capture
//     path is a nil-check only, with zero RNG draws and zero simulation
//     mutation, so capture OFF leaves the simulation trace and digest
//     byte-identical to an uncaptured run.
//   - Capture ON is deterministic under a fixed seed: Recorder assigns each
//     event a monotonic Seq in Record call order, and GoDES simulations are
//     single-goroutine and pump-driven, so call order IS sim order. No
//     wall-clock and no goroutine interleaving enters the record.
//   - Capture output is EXCLUDED from the simulation digest. A Recorder is
//     observational: it never registers mechanisms as topology or monitor
//     nodes, and the monitor/metrics digest serializes metrics only — so
//     the sim trace and digest with capture ON are byte-identical to
//     capture OFF.
//   - Run stays I/O-free. The recorder buffers in RAM during the sim; the
//     caller flushes (Recorder.FlushTo, JSONL sorted by Seq) after Run.
//
// The recorder never uses time.Now and never spawns goroutines; every
// integer is sim time or a count. Sim-time arithmetic is the core's
// saturating SimTime; event ts values are int64 projections of sim time.
package capture
