package faults

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/lawRathod/queuesim/model"
)

// ActivityFailsAt reports whether an activity completing at nowMs fails:
// the decision vector's activity_failure window, else the config default
// fault window. The config window applies regardless of the vector kind
// ONLY for add_drop — in the bubble, the add_drop confirm keeps the stock
// activity-failure window active (the merged config has both drop_rate AND
// the fault window). For all other vectors, the config window is
// suppressed (the vector's own window is the only failure source).
// Fault-control (taxonomy in models/temporal/doc.go): a test knob at the
// activity-failure decision chokepoint, not a server pin.
func ActivityFailsAt(spec Spec, nowMs int64) bool {
	if spec.Vector != nil {
		if spec.Vector.Kind == model.KindActivityFailure {
			return spec.Vector.ActiveAt(nowMs)
		}
		for _, m := range spec.Vector.Members() {
			if m.Kind == model.KindActivityFailure && m.ActiveAt(nowMs) {
				return true
			}
		}
		if spec.Vector.Kind != model.KindAddDrop {
			return false
		}
	}
	if spec.FaultStartMs == 0 && spec.FaultEndMs == 0 {
		return false
	}
	if nowMs < spec.FaultStartMs {
		return false
	}
	if spec.FaultEndMs == 0 {
		return true
	}
	return nowMs < spec.FaultEndMs
}

// ActivityFailsFor reports whether a SPECIFIC workflow (by ID) fails its
// activity at nowMs. In cascade mode, bad actors (ID hash < fraction) fail
// during the default fault window; healthy workflows never fail.
func ActivityFailsFor(spec Spec, id uint64, nowMs int64) bool {
	if !spec.CascadeOn {
		return ActivityFailsAt(spec, nowMs)
	}
	if !ActivityFailsAt(spec, nowMs) {
		return false
	}
	h := fnv.New32a()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], id)
	h.Write(buf[:])
	return float64(h.Sum32())/float64(^uint32(0)) < spec.CascadeBadFraction
}
