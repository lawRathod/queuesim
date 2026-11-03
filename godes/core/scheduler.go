package core

import "container/heap"

type scheduledEvent struct {
	at     SimTime
	seq    uint64
	taskID TaskID
	kind   FrontierEventKind
	compID string
	name   string
	fn     Event
}

type eventHeap struct {
	data []scheduledEvent
}

func newEventHeap() *eventHeap {
	h := &eventHeap{data: make([]scheduledEvent, 0)}
	heap.Init(h)
	return h
}

func (h *eventHeap) Len() int { return len(h.data) }

func (h *eventHeap) Less(i, j int) bool {
	a := h.data[i]
	b := h.data[j]
	if a.at != b.at {
		return a.at < b.at
	}
	return a.seq < b.seq
}

func (h *eventHeap) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *eventHeap) Push(x any) {
	h.data = append(h.data, x.(scheduledEvent))
}

func (h *eventHeap) Pop() any {
	n := len(h.data)
	x := h.data[n-1]
	h.data = h.data[:n-1]
	return x
}

func (h *eventHeap) push(ev scheduledEvent) {
	heap.Push(h, ev)
}

func (h *eventHeap) pop() (scheduledEvent, bool) {
	if h.Len() == 0 {
		return scheduledEvent{}, false
	}
	return heap.Pop(h).(scheduledEvent), true
}

// peek returns the minimum element without removing it.
func (h *eventHeap) peek() (scheduledEvent, bool) {
	if h.Len() == 0 {
		return scheduledEvent{}, false
	}
	return h.data[0], true
}

// hasTaskID reports whether any event in the heap has the given TaskID.
func (h *eventHeap) hasTaskID(id TaskID) bool {
	for _, ev := range h.data {
		if ev.taskID == id {
			return true
		}
	}
	return false
}
