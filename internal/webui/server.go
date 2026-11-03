// Package webui serves the interactive dashboard: the scenario catalog, a
// synchronous run endpoint that returns a complete trace, a seed sweep,
// and the embedded frontend.
package webui

import (
	"compress/gzip"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"runtime"
	"strings"

	"github.com/lawRathod/queuesim/godes/seed"
	"github.com/lawRathod/queuesim/internal/lab"
)

//go:embed assets
var assetsFS embed.FS

// maxBodyBytes bounds request bodies; the largest legitimate request is a
// knob map of a few dozen floats.
const maxBodyBytes = 1 << 20

// Options configures the server.
type Options struct {
	// MaxConcurrentRuns bounds simultaneous simulations (each is CPU-bound).
	// Default 2.
	MaxConcurrentRuns int
	// Version is reported by /api/meta and the footer. Default "dev".
	Version string
	// Logger receives request errors. Default slog.Default().
	Logger *slog.Logger
}

// New returns the dashboard handler.
func New(opts Options) http.Handler {
	if opts.MaxConcurrentRuns <= 0 {
		opts.MaxConcurrentRuns = 2
	}
	if opts.Version == "" {
		opts.Version = "dev"
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	s := &server{opts: opts, sem: make(chan struct{}, opts.MaxConcurrentRuns)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /api/meta", s.meta)
	mux.HandleFunc("GET /api/scenarios", s.scenarios)
	mux.HandleFunc("POST /api/runs", s.run)
	mux.HandleFunc("POST /api/runs/sweep", s.sweep)
	mux.HandleFunc("/", s.static)
	return withHeaders(withGzip(recoverer(opts.Logger, mux)))
}

type server struct {
	opts Options
	sem  chan struct{}
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) meta(w http.ResponseWriter, r *http.Request) {
	var dst int64
	hasDst := false
	if v, ok := seed.Global(); ok {
		dst, hasDst = v, true
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version":             s.opts.Version,
		"go":                  runtime.Version(),
		"dstseed":             dst,
		"dstseed_set":         hasDst,
		"max_concurrent_runs": s.opts.MaxConcurrentRuns,
	})
}

func (s *server) scenarios(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"scenarios": lab.Catalog()})
}

func (s *server) run(w http.ResponseWriter, r *http.Request) {
	var req lab.Request
	if err := decode(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.acquire() {
		writeError(w, http.StatusTooManyRequests, "two simulations are already running; try again in a moment")
		return
	}
	defer s.release()

	res, err := lab.Execute(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// sweepRequest is the wire shape of a determinism sweep: count runs with
// seeds start_seed .. start_seed+count-1 under the same knobs.
type sweepRequest struct {
	Scenario  string             `json:"scenario"`
	Knobs     map[string]float64 `json:"knobs,omitempty"`
	Count     int                `json:"count"`
	StartSeed int64              `json:"start_seed"`
}

func (s *server) sweep(w http.ResponseWriter, r *http.Request) {
	var req sweepRequest
	if err := decode(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.Count < 1 || req.Count > 32 {
		writeError(w, http.StatusBadRequest, "count must be in [1, 32]")
		return
	}
	seeds := make([]int64, req.Count)
	for i := range seeds {
		seeds[i] = req.StartSeed + int64(i)
	}
	if !s.acquire() {
		writeError(w, http.StatusTooManyRequests, "two simulations are already running; try again in a moment")
		return
	}
	defer s.release()

	points, err := lab.Sweep(req.Scenario, req.Knobs, seeds)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"points": points})
}

func (s *server) acquire() bool {
	select {
	case s.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *server) release() { <-s.sem }

// static serves the embedded frontend. Only known asset names are served;
// unknown paths fall back to index.html so a hash fragment or a deep link
// still opens the app.
func (s *server) static(w http.ResponseWriter, r *http.Request) {
	sub, err := fs.Sub(assetsFS, "assets")
	if err != nil {
		http.Error(w, "assets unavailable", http.StatusInternalServerError)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/")
	if name == "" {
		name = "index.html"
	}
	if _, err := fs.Stat(sub, name); err != nil {
		name = "index.html"
	}
	w.Header().Set("Cache-Control", "no-cache")
	http.ServeFileFS(w, r, sub, name)
}

func decode(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("request body is required")
		}
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if dec.More() {
		return fmt.Errorf("unexpected trailing data")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// The response is already committed; the client sees a truncated body.
		slog.Default().Error("write json", "err", err)
	}
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// recoverer turns a handler panic into a 500 instead of killing the
// connection (http.Server recovers already, but it logs nothing useful and
// closes the connection).
func recoverer(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				log.Error("handler panic", "path", r.URL.Path, "panic", fmt.Sprint(v))
				writeError(w, http.StatusInternalServerError, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func withHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self'")
		next.ServeHTTP(w, r)
	})
}

// gzipWriter drops the uncompressed Content-Length (http.ServeFileFS sets
// one from the file size) before the first write.
type gzipWriter struct {
	http.ResponseWriter
	gz          *gzip.Writer
	wroteHeader bool
}

func (g *gzipWriter) WriteHeader(code int) {
	if !g.wroteHeader {
		g.Header().Del("Content-Length")
		g.wroteHeader = true
	}
	g.ResponseWriter.WriteHeader(code)
}

func (g *gzipWriter) Write(p []byte) (int, error) {
	if !g.wroteHeader {
		g.Header().Del("Content-Length")
	}
	return g.gz.Write(p)
}

func withGzip(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}
		gz := gzip.NewWriter(w)
		defer gz.Close()
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Vary", "Accept-Encoding")
		w.Header().Del("Content-Length")
		next.ServeHTTP(&gzipWriter{ResponseWriter: w, gz: gz}, r)
	})
}
