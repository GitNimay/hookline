package domain

import (
	"errors"
	"testing"
)

func TestIDRoundTrip(t *testing.T) {
	id := NewID()
	s := FormatID(PrefixEvent, id)
	got, err := ParseID(PrefixEvent, s)
	if err != nil || got != id {
		t.Fatalf("round trip: %v %v", got, err)
	}
	for _, bad := range []string{"", "evt_", "ep_" + s[4:], s + "0", "evt_zz" + s[6:]} {
		if _, err := ParseID(PrefixEvent, bad); !errors.Is(err, ErrNotFound) {
			t.Errorf("ParseID(%q) = %v, want ErrNotFound", bad, err)
		}
	}
}

func TestMatchesEventType(t *testing.T) {
	cases := []struct {
		subs []string
		typ  string
		want bool
	}{
		{[]string{"order.created"}, "order.created", true},
		{[]string{"order.created"}, "order.paid", false},
		{[]string{"*"}, "anything", true},
		{nil, "x", false},
	}
	for _, c := range cases {
		if got := MatchesEventType(c.subs, c.typ); got != c.want {
			t.Errorf("MatchesEventType(%v,%q)=%v", c.subs, c.typ, got)
		}
	}
}

func TestFieldErrorIsInvalid(t *testing.T) {
	if !errors.Is(Invalid("url", "bad"), ErrInvalidArgument) {
		t.Fatal("FieldError should match ErrInvalidArgument")
	}
}
