package dists

import (
	"math"
	"testing"
)

// Compile-time check: mockProvider satisfies RandomProvider.
var _ RandomProvider = (*mockProvider)(nil)

type mockProvider struct {
	calls []struct {
		site DrawSite
		rate float64
	}
}

func (m *mockProvider) SampleExponential(site DrawSite, rate float64) float64 {
	m.calls = append(m.calls, struct {
		site DrawSite
		rate float64
	}{site, rate})
	return 42.0
}

func TestRandomProviderInterface(t *testing.T) {
	var p RandomProvider = &mockProvider{}
	site := DrawSite{Tag: "arrival", SiteID: 123}
	val := p.SampleExponential(site, 5.0)
	if val != 42.0 {
		t.Fatalf("expected 42.0, got %f", val)
	}
}

func TestDrawSiteFields(t *testing.T) {
	site := DrawSite{Tag: "service", SiteID: 456}
	if site.Tag != "service" {
		t.Fatalf("expected tag 'service', got '%s'", site.Tag)
	}
	if site.SiteID != 456 {
		t.Fatalf("expected SiteID 456, got %d", site.SiteID)
	}
}

func TestNewDrawSite(t *testing.T) {
	site := NewDrawSite("arrival")
	if site.Tag != "arrival" {
		t.Fatalf("expected tag 'arrival', got '%s'", site.Tag)
	}
	if site.SiteID == 0 {
		t.Fatal("expected non-zero SiteID from FNV-1a hash")
	}
	// Same tag must produce the same SiteID (determinism).
	site2 := NewDrawSite("arrival")
	if site.SiteID != site2.SiteID {
		t.Fatalf("expected same SiteID for same tag, got %d vs %d", site.SiteID, site2.SiteID)
	}
	// Different tags must produce different SiteIDs.
	site3 := NewDrawSite("service")
	if site.SiteID == site3.SiteID {
		t.Fatal("expected different SiteID for different tags")
	}
}

func TestDrawSiteZeroValue(t *testing.T) {
	var site DrawSite
	if site.Tag != "" {
		t.Fatalf("expected empty tag, got '%s'", site.Tag)
	}
	if site.SiteID != 0 {
		t.Fatalf("expected SiteID 0, got %d", site.SiteID)
	}
}

func TestMockProviderRecordsCalls(t *testing.T) {
	m := &mockProvider{}
	m.SampleExponential(DrawSite{Tag: "a", SiteID: 1}, 1.0)
	m.SampleExponential(DrawSite{Tag: "b", SiteID: 2}, 2.0)
	if len(m.calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(m.calls))
	}
	if m.calls[0].rate != 1.0 {
		t.Fatalf("expected rate 1.0, got %f", m.calls[0].rate)
	}
	if m.calls[1].site.Tag != "b" {
		t.Fatalf("expected tag 'b', got '%s'", m.calls[1].site.Tag)
	}
}

// TestRandomFromFunc_ComputesExponential verifies the adapter
// computes an exponential draw from a uniform float64.
func TestRandomFromFunc_ComputesExponential(t *testing.T) {
	calls := 0
	r := &RandomFromFunc{Float64: func() float64 {
		calls++
		return 0.5 // -ln(0.5) = ln(2) ≈ 0.693
	}}
	// rate=1.0 events/sec -> sample = 0.693/1.0 = 0.693 seconds
	got := r.SampleExponential(DrawSite{Tag: "test"}, 1.0)
	want := math.Log(2)
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("sample = %f, want %f", got, want)
	}
	if calls != 1 {
		t.Errorf("expected 1 call, got %d", calls)
	}
}

// TestRandomFromFunc_ClampsZeroU verifies that u=0 (which would
// produce -ln(1)/rate = -Inf) is clamped to a tiny positive value
// to avoid +Inf in the result.
func TestRandomFromFunc_ClampsZeroU(t *testing.T) {
	r := &RandomFromFunc{Float64: func() float64 { return 0 }}
	got := r.SampleExponential(DrawSite{Tag: "test"}, 1.0)
	// With u=1e-9, sample = -ln(0.999999999)/1.0 ≈ 1e-9
	if math.IsInf(got, +1) || got > 1e-6 {
		t.Errorf("sample = %f, want finite small value", got)
	}
}
