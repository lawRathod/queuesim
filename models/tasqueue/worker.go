package tasqueue

import (
	"github.com/lawRathod/queuesim/model"
)

// workerService returns the worker Service: the consumer fleet that pulls
// dispatched jobs off the broker. The real mechanism is one model.Poller
// (the per-queue worker pool; TaskOpts.Concurrency bounds in-flight work
// per queue — server.go:74-81) with a poll cadence of PollMs and an
// in-flight budget of Concurrency × NumWorkers, executing each attempt
// after ServiceMs and detecting JobOpts.Timeout as ServiceMs > TimeoutMs
// (server.go:388-390: the handler context deadline expires iff the service
// time exceeds the timeout). The Poller is constructed directly on the
// shared runtime's simulation below; worker declares NO ports and wires NO
// builder-queue shells — dispatch and completion/retry flow through the
// runtime mediator (the honest-schematics principle: if the code doesn't
// traverse it, the diagram doesn't draw it).
func workerService(rt *runtime) model.Service {
	return model.Service{
		Name:  "worker",
		Kind:  "tasqueue.worker",
		Label: "consumer fleet: poll, execute, timeout/fail/retry",
		Build: func(b *model.TopologyBuilder) error {
			rt.poller = model.NewPoller(rt.sim, model.PollerConfig{
				Name:           "tasqueue",
				PollIntervalMs: rt.cfg.PollMs,
				InFlightBudget: rt.cfg.Concurrency * rt.cfg.NumWorkers,
				EndMs:          rt.cfg.SimEndMs,
			}, rt.take)
			return nil
		},
	}
}
