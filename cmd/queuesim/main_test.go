package main

import "testing"

func TestParseKnobs(t *testing.T) {
	got, err := parseKnobs("arrival_rate=20, pollers_act=1,retry_coeff=1.5")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]float64{"arrival_rate": 20, "pollers_act": 1, "retry_coeff": 1.5}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	if _, err := parseKnobs("empty"); err == nil {
		t.Error("missing = accepted")
	}
	if _, err := parseKnobs("x=notanumber"); err == nil {
		t.Error("bad value accepted")
	}
	if got, err := parseKnobs(""); err != nil || len(got) != 0 {
		t.Errorf("empty spec: %v %v", got, err)
	}
}
