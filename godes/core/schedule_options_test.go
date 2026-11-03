package core

import (
	"bytes"
	"log/slog"
	"testing"
)

func TestDefaultScheduleOptions(t *testing.T) {
	opts := ScheduleOptions{}
	if opts.Kind != KindCallback {
		t.Fatalf("default Kind = %d, want KindCallback (%d)", opts.Kind, KindCallback)
	}
	if opts.TaskID != 0 {
		t.Fatalf("default TaskID = %d, want 0 (auto-assign)", opts.TaskID)
	}
	if opts.ComponentID != "" {
		t.Fatalf("default ComponentID = %q, want empty", opts.ComponentID)
	}
	if opts.Name != "" {
		t.Fatalf("default Name = %q, want empty", opts.Name)
	}
}

func TestFrontierEventKindValues(t *testing.T) {
	if KindCallback != 0 {
		t.Fatalf("KindCallback should be 0 (iota), got %d", KindCallback)
	}
	if KindTask != 1 {
		t.Fatalf("KindTask should be 1, got %d", KindTask)
	}
}

func TestScheduleAtExWithNameLogsBreadcrumb(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	sim, _ := NewSimulation(1, FIFOPolicy{})
	sim.SetLogger(logger)
	sim.ScheduleAtEx(NewSimTime(5), func(*Simulation) {}, ScheduleOptions{Name: "broker.enqueue"})
	sim.Run()
	if !bytes.Contains(buf.Bytes(), []byte("broker.enqueue")) {
		t.Fatalf("expected log to contain Name breadcrumb, got: %s", buf.String())
	}
}
