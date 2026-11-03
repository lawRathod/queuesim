// Package dists provides deterministic probability distributions for use in
// simulation models. It mirrors the DesCartes des-core dists.rs architecture.
//
// The package provides:
//   - RandomProvider interface for injectable sampling (enables tracing/replay)
//   - DrawSite for stable identification of sampling locations
//   - ExponentialDistribution for sampling exponential inter-arrival/service times
//
// Distribution constructors accept a seed for deterministic substream derivation
// via FNV-1a, matching the pattern in GoDES/core/rng.go.
package dists
