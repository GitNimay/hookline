package breaker

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestOpensAfterConsecutiveFailures(t *testing.T) {
	var s State
	now := t0
	for i := 1; i < ConsecutiveToOpen; i++ {
		if tr := s.Record(now, false, false); tr != "" {
			t.Fatalf("opened early at %d", i)
		}
	}
	if tr := s.Record(now, false, false); tr != Open {
		t.Fatalf("want open, got %q", tr)
	}
	d := s.Check(now)
	if d.Allow || !d.RetryAt.Equal(now.Add(BaseOpen)) {
		t.Fatalf("open breaker allowed: %+v", d)
	}
}

func TestOpensOnFailureRate(t *testing.T) {
	var s State
	now := t0
	// Alternate fail/success: consecutive never reaches 10, but 11/21 > 50%.
	var tr string
	for i := range 21 {
		tr = s.Record(now.Add(time.Duration(i)*time.Second), i%2 == 0, false)
		if tr == Open {
			break
		}
	}
	if tr == Open {
		t.Fatal("should not open with exactly <=50% failures")
	}
	s = State{}
	for i := range 20 {
		tr = s.Record(now.Add(time.Duration(i)*time.Second), i%3 == 0, false) // 2/3 failures
		if tr == Open {
			return
		}
	}
	t.Fatal("did not open at 66% failure rate over 20 attempts")
}

func TestOldFailuresLeaveTheWindow(t *testing.T) {
	var s State
	for i := range 15 {
		s.Record(t0.Add(time.Duration(i)*time.Second), i%4 != 0, false)
	}
	s.Consecutive = 0
	// Two minutes later the window is empty, so a few failures do not open it.
	later := t0.Add(2 * time.Minute)
	for i := range 5 {
		if tr := s.Record(later.Add(time.Duration(i)*time.Second), false, false); tr != "" {
			t.Fatal("stale window counted")
		}
	}
}

func TestHalfOpenProbeAndDoubling(t *testing.T) {
	var s State
	now := t0
	for range ConsecutiveToOpen {
		s.Record(now, false, false)
	}
	if s.View(now.Add(BaseOpen)) != HalfOpen {
		t.Fatal("want half-open after timer")
	}
	now = now.Add(BaseOpen)
	d := s.Check(now)
	if !d.Allow || !d.NeedProbe {
		t.Fatalf("half-open must allow one probe: %+v", d)
	}
	// Non-probe results while not closed are ignored.
	if tr := s.Record(now, true, false); tr != "" {
		t.Fatal("non-probe closed the breaker")
	}
	// Probe fails: reopen for double the time.
	if tr := s.Record(now, false, true); tr != Open {
		t.Fatal("failed probe should reopen")
	}
	if got := time.UnixMilli(s.OpenUntil).Sub(now); got != 2*BaseOpen {
		t.Fatalf("reopen duration %v want %v", got, 2*BaseOpen)
	}
	// Probe succeeds: closed and reset.
	now = time.UnixMilli(s.OpenUntil)
	if tr := s.Record(now, true, true); tr != Closed {
		t.Fatal("successful probe should close")
	}
	if s.Opens != 0 || s.Consecutive != 0 || s.View(now) != Closed {
		t.Fatalf("not reset: %+v", s)
	}
}

func TestOpenDurationCapped(t *testing.T) {
	s := State{Opens: 20}
	for range ConsecutiveToOpen {
		s.Record(t0, false, false)
	}
	if got := time.UnixMilli(s.OpenUntil).Sub(t0); got != MaxOpen {
		t.Fatalf("got %v want cap %v", got, MaxOpen)
	}
}
