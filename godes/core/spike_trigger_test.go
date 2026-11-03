// SPIKE probe (feasibility spike — removable): does key-driven ordering of
// coincident effect submissions produce a stable execution order when the
// submissions race from concurrent goroutines? See AGENTS spike task, Part 1.
//
// Submission order is FORCED to vary across runs (seeded per-run shuffle of
// each goroutine's effect order + 8 goroutines racing on the scheduler
// mutex). If key-driven ordering works, executed order must be immune.

package core

import (
	"fmt"
	mathrand "math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"testing"
)

const (
	spikeN    = 8  // concurrent submitting goroutines
	spikeM    = 20 // effects per goroutine
	spikeRuns = 20
	spikeAt   = 100 // all effects coincident at this sim time
)

// shufflePerRun returns a deterministic-but-different-per-run permutation of
// each goroutine's effect order, so submission (and thus scheduler seq
// assignment) genuinely differs across runs.
func shufflePerRun(r int) map[int][]int {
	prng := mathrand.New(mathrand.NewPCG(uint64(r+1), uint64(r+1)^0x9E3779B97F4A7C15))
	perm := make(map[int][]int, spikeN)
	for i := 0; i < spikeN; i++ {
		perm[i] = prng.Perm(spikeM)
	}
	return perm
}

// TestSpikeTriggerOrderStability: every effect carries a DISTINCT
// deterministic (ComponentID, TaskID) key. Assert the full executed sequence
// is identical across runs AND equals the key-sorted order, despite
// per-run submission order variation.
func TestSpikeTriggerOrderStability(t *testing.T) {
	prev := runtime.GOMAXPROCS(spikeN)
	defer runtime.GOMAXPROCS(prev)

	expected := make([]string, 0, spikeN*spikeM)
	for i := 0; i < spikeN; i++ {
		for j := 0; j < spikeM; j++ {
			expected = append(expected, fmt.Sprintf("g%d:e%d", i, j))
		}
	}

	var baseline []string
	for r := 0; r < spikeRuns; r++ {
		sim, err := NewSimulation(int64(r+1), FIFOPolicy{})
		if err != nil {
			t.Fatalf("NewSimulation: %v", err)
		}
		var order []string
		var wg sync.WaitGroup
		for i := 0; i < spikeN; i++ {
			js := shufflePerRun(r)[i] // different submission order every run
			wg.Add(1)
			go func(i int, js []int) {
				defer wg.Done()
				comp := fmt.Sprintf("g%d", i)
				for _, j := range js {
					key := fmt.Sprintf("g%d:e%d", i, j)
					sim.ScheduleAtEx(spikeAt, func(s *Simulation) {
						order = append(order, key)
					}, ScheduleOptions{
						ComponentID: comp,
						TaskID:      TaskID(i*1000 + j + 1), // nonzero: mint-on-zero collision
					})
				}
			}(i, js)
		}
		wg.Wait()
		sim.RunUntil(spikeAt)
		if len(order) != spikeN*spikeM {
			t.Fatalf("run %d: executed %d effects, want %d", r, len(order), spikeN*spikeM)
		}
		if r == 0 {
			baseline = slices.Clone(order)
		} else if !slices.Equal(order, baseline) {
			t.Fatalf("run %d: execution order differs from baseline run 0", r)
		}
	}
	if !slices.Equal(baseline, expected) {
		t.Fatalf("SPIKE: order not key-driven; want %v, got %v", expected, baseline)
	}
	t.Logf("SPIKE: full per-effect order stable across %d runs (submission order shuffled per run), key-driven sorted", spikeRuns)
}

// TestSpikeTriggerKeyGranularityStability: key = ComponentID only (goroutine
// id), TaskID minted by the scheduler (submission/lock order). Assert the
// key-granularity execution order is stable; report whether the intra-key
// per-effect order varies run to run.
func TestSpikeTriggerKeyGranularityStability(t *testing.T) {
	prev := runtime.GOMAXPROCS(spikeN)
	defer runtime.GOMAXPROCS(prev)

	wantComp := make([]string, 0, spikeN*spikeM)
	for i := 0; i < spikeN; i++ {
		for j := 0; j < spikeM; j++ {
			wantComp = append(wantComp, fmt.Sprintf("g%d", i))
		}
	}

	fullSeqs := map[string]int{}
	var compBaseline []string
	for r := 0; r < spikeRuns; r++ {
		sim, err := NewSimulation(int64(r+1), FIFOPolicy{})
		if err != nil {
			t.Fatalf("NewSimulation: %v", err)
		}
		var compOrder, fullOrder []string
		var wg sync.WaitGroup
		for i := 0; i < spikeN; i++ {
			js := shufflePerRun(r)[i]
			wg.Add(1)
			go func(i int, js []int) {
				defer wg.Done()
				comp := fmt.Sprintf("g%d", i)
				for _, j := range js {
					token := fmt.Sprintf("g%d:e%d", i, j)
					sim.ScheduleAtEx(spikeAt, func(s *Simulation) {
						compOrder = append(compOrder, comp)
						fullOrder = append(fullOrder, token)
					}, ScheduleOptions{ComponentID: comp})
				}
			}(i, js)
		}
		wg.Wait()
		sim.RunUntil(spikeAt)
		if r == 0 {
			compBaseline = slices.Clone(compOrder)
		} else if !slices.Equal(compOrder, compBaseline) {
			t.Fatalf("run %d: key-granularity order differs from baseline", r)
		}
		fullSeqs[fmt.Sprint(fullOrder)]++
	}
	if !slices.Equal(compBaseline, wantComp) {
		t.Fatalf("SPIKE: key-granularity order not key-driven; want %v, got %v", wantComp, compBaseline)
	}
	t.Logf("SPIKE: key-granularity (ComponentID) order stable across %d runs despite shuffled submission", spikeRuns)
	t.Logf("SPIKE: distinct intra-key per-effect sequences observed: %d of %d runs (minted TaskID = submission order, not key-driven)", len(fullSeqs), spikeRuns)
}
