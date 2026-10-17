package kv

import (
	"encoding/json"
	"testing"

	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/raft"
)

func apply(t *testing.T, f *FSM, idx uint64, c Command) Result {
	t.Helper()
	b, _ := json.Marshal(c)
	return f.Apply(raft.Entry{Index: idx, Data: b}).(Result)
}

func ptr(s string) *string { return &s }

func TestFSMOperations(t *testing.T) {
	f := NewFSM()
	if r := apply(t, f, 1, Command{Op: OpPut, Key: "a", Value: "1"}); !r.OK || r.Existed {
		t.Fatalf("put: %+v", r)
	}
	if r := apply(t, f, 2, Command{Op: OpPut, Key: "a", Value: "2"}); r.Prev != "1" || !r.Existed {
		t.Fatalf("overwrite: %+v", r)
	}
	if r := apply(t, f, 3, Command{Op: OpCAS, Key: "a", Expected: ptr("nope"), Value: "3"}); r.OK {
		t.Fatalf("cas with wrong expected succeeded: %+v", r)
	}
	if r := apply(t, f, 4, Command{Op: OpCAS, Key: "a", Expected: ptr("2"), Value: "3"}); !r.OK {
		t.Fatalf("cas failed: %+v", r)
	}
	if r := apply(t, f, 5, Command{Op: OpCAS, Key: "new", Value: "x"}); !r.OK {
		t.Fatalf("cas create-if-absent failed: %+v", r)
	}
	if r := apply(t, f, 6, Command{Op: OpCAS, Key: "new", Value: "y"}); r.OK {
		t.Fatalf("cas create-if-absent overwrote: %+v", r)
	}
	if r := apply(t, f, 7, Command{Op: OpDelete, Key: "a"}); !r.OK {
		t.Fatalf("delete: %+v", r)
	}
	if _, ok := f.Get("a"); ok {
		t.Fatal("deleted key still present")
	}
	if r := apply(t, f, 8, Command{Op: "bogus", Key: "a"}); r.Err == "" {
		t.Fatal("unknown op accepted")
	}
	if r := f.Apply(raft.Entry{Index: 9, Data: []byte("{")}).(Result); r.Err == "" {
		t.Fatal("garbage accepted")
	}
	if got := f.Keys("", 10); len(got) != 1 || got[0] != "new" {
		t.Fatalf("keys = %v", got)
	}
}

func TestFSMSessionsAreExactlyOnce(t *testing.T) {
	f := NewFSM()
	c := Command{Op: OpCAS, Key: "k", Value: "v", ClientID: "c1", Seq: 1}
	first := apply(t, f, 1, c)
	// The leader crashed after commit but before replying; the client
	// retries the same request, which lands at a new index.
	retry := apply(t, f, 2, c)
	if !first.OK || !retry.OK || retry.Index != first.Index {
		t.Fatalf("retry not deduplicated: first=%+v retry=%+v", first, retry)
	}
	if r := apply(t, f, 3, Command{Op: OpCAS, Key: "k", Value: "w", ClientID: "c1", Seq: 2}); r.OK {
		t.Fatalf("new seq should run (and fail CAS): %+v", r)
	}
}

func TestFSMSnapshotRoundTrip(t *testing.T) {
	f := NewFSM()
	apply(t, f, 1, Command{Op: OpPut, Key: "a", Value: "1", ClientID: "c", Seq: 7})
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	g := NewFSM()
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if v, _ := g.Get("a"); v != "1" {
		t.Fatalf("restored value %q", v)
	}
	// Session table survived: a stale retry is still deduplicated.
	if r := apply(t, g, 2, Command{Op: OpPut, Key: "a", Value: "dup", ClientID: "c", Seq: 7}); r.Index != 1 {
		t.Fatalf("session lost across snapshot: %+v", r)
	}
	if v, _ := g.Get("a"); v != "1" {
		t.Fatalf("duplicate applied after restore: %q", v)
	}
	if err := g.Restore([]byte("nope")); err == nil {
		t.Fatal("bad snapshot accepted")
	}
}
