// Package seed owns GoDES's single run-seed contract (ADR-122): one number
// identifies a run on every leg, because a failure found in one run is only
// replayable if the run is replayable.
//
// The number is supplied by the DSTSEED environment variable — already the
// variable the ADR-119 runtime overlay reads in schedinit before main. The
// overlay's copy of the seed cannot be changed by main, so the environment
// wins over the config: Config.Seed becomes "the per-leg default used when no
// global seed is given". With DSTSEED unset nothing changes, so every recorded
// artifact stays valid.
//
// Roles (ADR-122). Only the run seed is resolved here:
//
//   - Run seed — resolved: Resolve at each leg's pointer-mutating run
//     boundary. The returned value selects which member of an EXISTING
//     deterministic stream family the run uses; it never adds a draw, reorders
//     an event, or reads a clock.
//   - Protocol/draw seeds — never resolved: the pre-registered sweep seeds
//     (DrawSeed, CalSeed, StabilitySeeds, HopperSeeds, blindSeedSet, …) are
//     frozen protocol inputs. GuardDeclared errors when a global seed is set
//     and disagrees with one of them.
//   - Derived substreams — untouched by construction: core.deriveSubstreamSeed,
//     core/dists, continuation seeds, raft's NodeSeedBase as an axis.
//
// The environment is read exactly once, behind a sync.Once, for the whole
// process. That is deliberate: os.Getenv is a pure function of process-mutable
// state, so a later t.Setenv would move a config seed while the runtime's
// schedinit copy stayed where it was — re-creating the split this package
// exists to close.
//
// This is a stdlib-only leaf package (no model, core or temporal import), so
// the bubble harnesses that link against the patched runtime can import it the
// way they import faultsched.
//
// Precedence and the unseeded path:
//
//	Resolve(x) = Global() when DSTSEED is set and non-zero, else Default(x)
//	Default(x) = x when x != 0, else DefaultRunSeed (42)
//
// Empty and "0" both count as UNSET. dstSetSeed(0) leaves dstSeedState == 0 and
// dstActive() == false, so at the runtime layer DSTSEED=0 IS unset; letting
// Resolve(0) return 0 would zero every leg's config seed while the seam stayed
// off, which is neither world.
package seed
