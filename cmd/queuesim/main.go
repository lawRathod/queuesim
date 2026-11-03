// Command queuesim runs the deterministic queue-network models: an
// interactive dashboard, a one-shot runner, and a trace exporter.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/lawRathod/queuesim/internal/lab"
	"github.com/lawRathod/queuesim/internal/webui"
)

// version is overridden at build time:
// go build -ldflags "-X main.version=v1.2.3" ./cmd/queuesim
var version = "dev"

func main() {
	if len(os.Args) < 2 {
		serve(os.Args[1:])
		return
	}
	switch os.Args[1] {
	case "serve", "web":
		serve(os.Args[2:])
	case "list":
		list(os.Args[2:])
	case "run":
		run(os.Args[2:])
	case "export":
		export(os.Args[2:])
	case "version", "-version", "--version":
		fmt.Printf("queuesim %s (%s %s/%s)\n", version, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "queuesim: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `queuesim — deterministic queue-network models of distributed systems

usage:
  queuesim [serve] [-addr 127.0.0.1:8080] [-open]   start the interactive dashboard
  queuesim list                                     list scenarios
  queuesim run -scenario ID [-seed N] [-set k=v,...] [-json]
                                                    run once and print the summary
  queuesim export -scenario ID [-seed N] [-set k=v,...] [-o file]
                                                    write the full trace as JSON
  queuesim version

knobs are the scenario's keys, e.g. -set arrival_rate=20,pollers_act=1
`)
}

func serve(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:8080", "listen address")
	open := fs.Bool("open", false, "open the dashboard in a browser")
	fs.Parse(args)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	handler := webui.New(webui.Options{Version: version, Logger: logger})
	srv := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	url := "http://" + displayAddr(*addr)
	fmt.Printf("\n  queuesim %s\n  dashboard  %s\n  scenarios  %s/api/scenarios\n\n  Ctrl-C to stop\n\n",
		version, url, url)
	if *open {
		openBrowser(url)
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		fatal(err)
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal(err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "shutdown:", err)
	}
}

func displayAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "localhost"
	}
	return net.JoinHostPort(host, port)
}

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}

func list(args []string) {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	fs.Parse(args)
	group := ""
	for _, sc := range lab.Catalog() {
		if sc.Group != group {
			group = sc.Group
			fmt.Printf("\n%s\n", group)
		}
		fmt.Printf("  %-16s %s\n", sc.ID, sc.Title)
		fmt.Printf("  %-16s %s\n", "", sc.Subtitle)
	}
	fmt.Println()
}

func run(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	scenario := fs.String("scenario", "", "scenario id (see: queuesim list)")
	seed := fs.Int64("seed", -1, "RNG seed (default: the scenario's seed)")
	knobs := fs.String("set", "", "comma-separated knob overrides, e.g. arrival_rate=20,pollers_act=1")
	asJSON := fs.Bool("json", false, "print the full run as JSON")
	fs.Parse(args)

	res := execute(*scenario, *seed, *knobs)
	if *asJSON {
		out, err := json.MarshalIndent(res, "", "  ")
		if err != nil {
			fatal(err)
		}
		fmt.Println(string(out))
		return
	}
	printSummary(res)
}

func export(args []string) {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	scenario := fs.String("scenario", "", "scenario id (see: queuesim list)")
	seed := fs.Int64("seed", -1, "RNG seed (default: the scenario's seed)")
	knobs := fs.String("set", "", "comma-separated knob overrides")
	out := fs.String("o", "", "output file (default: stdout)")
	fs.Parse(args)

	res := execute(*scenario, *seed, *knobs)
	data, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		fatal(err)
	}
	if *out == "" {
		fmt.Println(string(data))
		return
	}
	if err := os.WriteFile(*out, append(data, '\n'), 0o644); err != nil {
		fatal(err)
	}
	fmt.Printf("wrote %s (%d bytes)\n", *out, len(data))
}

func execute(scenario string, seed int64, knobSpec string) *lab.Run {
	if scenario == "" {
		fatal(errors.New("-scenario is required (see: queuesim list)"))
	}
	req := lab.Request{Scenario: scenario, Knobs: map[string]float64{}}
	if seed >= 0 {
		req.Seed = &seed
	}
	vals, err := parseKnobs(knobSpec)
	if err != nil {
		fatal(err)
	}
	req.Knobs = vals
	res, err := lab.Execute(req)
	if err != nil {
		fatal(err)
	}
	return res
}

func parseKnobs(spec string) (map[string]float64, error) {
	out := map[string]float64{}
	if strings.TrimSpace(spec) == "" {
		return out, nil
	}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("bad knob %q (want key=value)", part)
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return nil, fmt.Errorf("bad value for %q: %w", k, err)
		}
		out[strings.TrimSpace(k)] = f
	}
	return out, nil
}

func printSummary(r *lab.Run) {
	fmt.Printf("\n%s  (%s, seed %d)\n", r.Scenario, r.Model, r.Seed)
	fmt.Printf("%s\n\n", strings.Repeat("-", 58))
	for _, m := range r.Metrics {
		unit := ""
		if m.Unit != "" {
			unit = " " + m.Unit
		}
		fmt.Printf("  %-28s %12s%s\n", m.Label, fmtNum(m.Value, m.Precision), unit)
	}
	for _, p := range r.Percentiles {
		fmt.Printf("\n  %s (n=%d)\n", p.Label, p.Count)
		fmt.Printf("    p50 %-8s p90 %-8s p99 %-8s max %s\n",
			ms(p.P50Ms), ms(p.P90Ms), ms(p.P99Ms), ms(p.MaxMs))
	}
	fmt.Printf("\n  digest  %s\n", r.Digest)
	fmt.Printf("  wall    %d ms · %d timeline samples · %d ms simulated\n\n", r.WallMs, len(r.T), r.SimEndMs)
}

func fmtNum(v float64, precision int) string {
	if precision == 2 {
		return strconv.FormatFloat(v, 'f', 2, 64)
	}
	if precision == 1 {
		return strconv.FormatFloat(v, 'f', 1, 64)
	}
	return strconv.FormatFloat(v, 'f', 0, 64)
}

func ms(v int64) string {
	if v >= 1000 {
		return strconv.FormatFloat(float64(v)/1000, 'f', 2, 64) + "s"
	}
	return strconv.FormatInt(v, 10) + "ms"
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "queuesim:", err)
	os.Exit(1)
}
