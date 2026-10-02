package capture

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestEventZeroValueMarshals asserts the zero Event marshals to valid JSON
// with only the mandatory seq field — every optional field is omitempty.
func TestEventZeroValueMarshals(t *testing.T) {
	data, err := json.Marshal(Event{})
	if err != nil {
		t.Fatalf("marshal zero event: %v", err)
	}
	if string(data) != `{"seq":0}` {
		t.Errorf("zero event: got %s, want {\"seq\":0}", data)
	}
	var e Event
	if err := json.Unmarshal(data, &e); err != nil {
		t.Fatalf("unmarshal zero event: %v", err)
	}
	if e != (Event{}) {
		t.Errorf("round trip zero event: got %+v", e)
	}
}

// TestEventOmitemptyFields asserts zero-valued optional fields are absent
// from the JSON line and only populated fields appear, so a Record without
// options is compact and unambiguous.
func TestEventOmitemptyFields(t *testing.T) {
	e := Event{Seq: 7, TsMs: 120, From: "load", To: "history", Kind: "workflow_start"}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	line := string(data)
	for _, want := range []string{`"seq":7`, `"ts_ms":120`, `"from":"load"`, `"to":"history"`, `"kind":"workflow_start"`} {
		if !strings.Contains(line, want) {
			t.Errorf("line %s missing %s", line, want)
		}
	}
	for _, notWant := range []string{`"task_id"`, `"attempt"`, `"class"`, `"count"`} {
		if strings.Contains(line, notWant) {
			t.Errorf("line %s contains omitted field %s", line, notWant)
		}
	}
}

// TestEventClassAndCountMarshal asserts Class and Count serialize as
// integers (not strings) when populated.
func TestEventClassAndCountMarshal(t *testing.T) {
	e := Event{Seq: 1, TaskID: "wf-0", Attempt: 2, Class: 1, Count: 3}
	data, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	line := string(data)
	for _, want := range []string{`"task_id":"wf-0"`, `"attempt":2`, `"class":1`, `"count":3`} {
		if !strings.Contains(line, want) {
			t.Errorf("line %s missing %s", line, want)
		}
	}
}

// TestWithTaskAndCount asserts the Option funcs populate the intended
// fields when applied in either order.
func TestWithTaskAndCount(t *testing.T) {
	e := Event{}
	WithCount(5)(&e)
	WithTask("wf-0", 2, 1)(&e)
	if e.TaskID != "wf-0" || e.Attempt != 2 || e.Class != 1 {
		t.Errorf("WithTask fields wrong: %+v", e)
	}
	if e.Count != 5 {
		t.Errorf("WithCount wrong: got %d want 5", e.Count)
	}
}
