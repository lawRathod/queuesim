package webui

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lawRathod/queuesim/internal/lab"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	h := New(Options{MaxConcurrentRuns: 1, Version: "test", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func TestStaticIndexAndAssets(t *testing.T) {
	srv := newTestServer(t)
	for _, path := range []string{"/", "/app.js", "/style.css", "/does-not-exist"} {
		res, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Errorf("%s: status %d", path, res.StatusCode)
		}
		if len(body) == 0 {
			t.Errorf("%s: empty body", path)
		}
		if path == "/" || path == "/does-not-exist" {
			if !bytes.Contains(body, []byte("queuesim")) {
				t.Errorf("%s: does not look like the dashboard", path)
			}
		}
	}
}

func TestFrontendIsSelfContained(t *testing.T) {
	srv := newTestServer(t)
	for _, path := range []string{"/", "/app.js", "/style.css"} {
		res, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		for _, banned := range []string{"src=\"http", "href=\"http", "cdn.", "unpkg", "jsdelivr"} {
			if bytes.Contains(body, []byte(banned)) {
				t.Errorf("%s references an external resource (%q); the dashboard must work offline", path, banned)
			}
		}
	}
}

func TestSecurityHeaders(t *testing.T) {
	srv := newTestServer(t)
	res, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if got := res.Header.Get("Content-Security-Policy"); got == "" {
		t.Error("missing CSP")
	}
	if got := res.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("nosniff = %q", got)
	}
}

func TestScenariosEndpoint(t *testing.T) {
	srv := newTestServer(t)
	res, err := http.Get(srv.URL + "/api/scenarios")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var payload struct {
		Scenarios []lab.Info `json:"scenarios"`
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Scenarios) < 10 {
		t.Fatalf("got %d scenarios", len(payload.Scenarios))
	}
	if payload.Scenarios[0].Knobs == nil {
		t.Error("scenario knobs missing")
	}
}

func TestRunEndpoint(t *testing.T) {
	srv := newTestServer(t)
	body := `{"scenario":"calm","seed":11,"knobs":{"arrival_rate":5}}`
	res, err := http.Post(srv.URL+"/api/runs", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status %d: %s", res.StatusCode, b)
	}
	var run lab.Run
	if err := json.NewDecoder(res.Body).Decode(&run); err != nil {
		t.Fatal(err)
	}
	if run.Digest == "" || len(run.T) == 0 || run.Scenario != "calm" || run.Seed != 11 {
		t.Fatalf("unexpected run: scenario=%q seed=%d frames=%d digest=%q", run.Scenario, run.Seed, len(run.T), run.Digest)
	}
	if run.Knobs["arrival_rate"] != 5 {
		t.Errorf("knob override not applied: %v", run.Knobs)
	}
	for _, s := range run.Series {
		if len(run.V[s.Key]) != len(run.T) {
			t.Errorf("series %q length mismatch", s.Key)
		}
	}
}

func TestRunEndpointRejectsBadInput(t *testing.T) {
	srv := newTestServer(t)
	cases := []struct {
		name string
		body string
	}{
		{"unknown scenario", `{"scenario":"nope"}`},
		{"unknown knob", `{"scenario":"calm","knobs":{"nope":1}}`},
		{"out of range", `{"scenario":"calm","knobs":{"pollers_wf":0}}`},
		{"malformed json", `{"scenario":`},
		{"trailing data", `{"scenario":"calm"}{"scenario":"calm"}`},
		{"empty body", ``},
	}
	for _, c := range cases {
		res, err := http.Post(srv.URL+"/api/runs", "application/json", strings.NewReader(c.body))
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", c.name, res.StatusCode)
		}
	}
}

func TestGzipResponse(t *testing.T) {
	srv := newTestServer(t)
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/scenarios", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	tr := &http.Transport{DisableCompression: true}
	client := &http.Client{Transport: tr}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("encoding = %q", res.Header.Get("Content-Encoding"))
	}
	zr, err := gzip.NewReader(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.NewDecoder(zr).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if _, ok := payload["scenarios"]; !ok {
		t.Error("gzipped payload is not the scenario catalog")
	}
}

func TestSweepEndpoint(t *testing.T) {
	srv := newTestServer(t)
	body := `{"scenario":"retry-storm","count":2,"start_seed":1,"knobs":{}}`
	res, err := http.Post(srv.URL+"/api/runs/sweep", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status %d: %s", res.StatusCode, b)
	}
	var payload struct {
		Points []lab.SweepPoint `json:"points"`
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Points) != 2 {
		t.Fatalf("got %d points", len(payload.Points))
	}
	if payload.Points[0].Seed != 1 || payload.Points[1].Seed != 2 {
		t.Errorf("seeds = %d,%d", payload.Points[0].Seed, payload.Points[1].Seed)
	}
}

func TestSweepBounds(t *testing.T) {
	srv := newTestServer(t)
	for _, body := range []string{
		`{"scenario":"calm","count":0}`,
		`{"scenario":"calm","count":33}`,
	} {
		res, err := http.Post(srv.URL+"/api/runs/sweep", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d", body, res.StatusCode)
		}
	}
}

func TestRunConcurrencyLimit(t *testing.T) {
	s := &server{sem: make(chan struct{}, 1)}
	if !s.acquire() {
		t.Fatal("first acquire failed")
	}
	if s.acquire() {
		t.Fatal("second acquire succeeded past the limit")
	}
	s.release()
	if !s.acquire() {
		t.Fatal("acquire after release failed")
	}
}
