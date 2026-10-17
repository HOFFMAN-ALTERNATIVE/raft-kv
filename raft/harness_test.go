package raft_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/raft"
	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/raft/memnet"
)

const (
	electionTimeout = 150 * time.Millisecond
	heartbeat       = 30 * time.Millisecond
)

// cluster is a test harness in the spirit of MIT 6.5840's config.go: it owns
// the nodes, their storage and state machines, and a global commit checker
// that fails the test the moment two nodes apply different commands at the
// same index (State Machine Safety, Figure 3).
type cluster struct {
	t       testing.TB
	net     *memnet.Network
	ids     []string
	snapAt  int
	mu      sync.Mutex
	nodes   map[string]*raft.Node
	stores  map[string]*raft.MemoryStorage
	fsms    map[string]*testFSM
	applied map[uint64]string // index -> command, across all nodes
	errs    []string
}

func newCluster(t testing.TB, n, snapshotThreshold int) *cluster {
	t.Helper()
	c := &cluster{
		t: t, net: memnet.New(), snapAt: snapshotThreshold,
		nodes: map[string]*raft.Node{}, stores: map[string]*raft.MemoryStorage{},
		fsms: map[string]*testFSM{}, applied: map[uint64]string{},
	}
	for i := range n {
		id := fmt.Sprintf("n%d", i+1)
		c.ids = append(c.ids, id)
		c.stores[id] = raft.NewMemoryStorage()
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(c.cleanup)
	return c
}

func (c *cluster) cleanup() {
	c.mu.Lock()
	nodes := make([]*raft.Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.mu.Unlock()
	for _, n := range nodes {
		n.Stop()
	}
	c.checkErrs()
}

func (c *cluster) checkErrs() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.errs) > 0 {
		c.t.Fatalf("safety violation: %v", c.errs)
	}
}

// start boots (or reboots after crash) node id from its persisted storage.
func (c *cluster) start(id string) {
	c.t.Helper()
	fsm := &testFSM{c: c, id: id, applied: map[uint64]string{}}
	node, err := raft.NewNode(raft.Config{
		ID: id, Peers: c.ids,
		ElectionTimeout: electionTimeout, HeartbeatInterval: heartbeat,
		SnapshotThreshold: c.snapAt,
	}, c.net.Transport(id), c.stores[id], fsm)
	if err != nil {
		c.t.Fatal(err)
	}
	c.mu.Lock()
	c.nodes[id], c.fsms[id] = node, fsm
	c.mu.Unlock()
	c.net.Register(id, node)
}

// crash stops a node and cuts it off; its storage survives.
func (c *cluster) crash(id string) {
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	c.net.Disconnect(id)
	if n != nil {
		n.Stop()
	}
}

func (c *cluster) node(id string) *raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// checkOneLeader waits for exactly one leader in the highest term among
// running nodes and fails if two nodes lead the same term.
func (c *cluster) checkOneLeader(among ...string) string {
	c.t.Helper()
	if len(among) == 0 {
		among = c.ids
	}
	for range 50 {
		time.Sleep(50 * time.Millisecond)
		leaders := map[uint64][]string{}
		for _, id := range among {
			n := c.node(id)
			if n == nil {
				continue
			}
			if term, ok := n.State(); ok {
				leaders[term] = append(leaders[term], id)
			}
		}
		var top uint64
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
			top = max(top, term)
		}
		if ls := leaders[top]; len(ls) == 1 {
			return ls[0]
		}
	}
	c.t.Fatalf("no leader elected among %v", among)
	return ""
}

func (c *cluster) checkNoLeader(among ...string) {
	c.t.Helper()
	for _, id := range among {
		if n := c.node(id); n != nil {
			if _, ok := n.State(); ok {
				c.t.Fatalf("%s is leader, expected none among %v", id, among)
			}
		}
	}
}

func (c *cluster) term(id string) uint64 {
	t, _ := c.node(id).State()
	return t
}

// nCommitted reports how many nodes applied a command at index.
func (c *cluster) nCommitted(index uint64) (int, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	count, cmd := 0, ""
	for _, f := range c.fsms {
		f.mu.Lock()
		v, ok := f.applied[index]
		f.mu.Unlock()
		if ok {
			count++
			cmd = v
		}
	}
	return count, cmd
}

// one submits cmd via whichever node leads, retrying through leader changes,
// and waits until at least want nodes applied it. Returns its log index.
func (c *cluster) one(cmd string, want int) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.ids {
			n := c.node(id)
			if n == nil {
				continue
			}
			if _, ok := n.State(); !ok {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			res, err := n.Propose(ctx, []byte(cmd))
			cancel()
			if err != nil {
				continue
			}
			idx := res.(uint64)
			for range 100 {
				if cnt, got := c.nCommitted(idx); cnt >= want && got == cmd {
					return idx
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("one(%q) failed to reach agreement on %d nodes", cmd, want)
	return 0
}

// testFSM records every applied command and checks it against all others.
type testFSM struct {
	c       *cluster
	id      string
	mu      sync.Mutex
	last    uint64
	applied map[uint64]string
}

func (f *testFSM) Apply(e raft.Entry) any {
	cmd := string(e.Data)
	f.mu.Lock()
	if e.Index <= f.last {
		f.c.fail("%s applied index %d after %d (out of order)", f.id, e.Index, f.last)
	}
	f.last = e.Index
	f.applied[e.Index] = cmd
	f.mu.Unlock()

	f.c.mu.Lock()
	if prev, ok := f.c.applied[e.Index]; ok && prev != cmd {
		f.c.errs = append(f.c.errs, fmt.Sprintf("index %d: %s applied %q, another node applied %q", e.Index, f.id, cmd, prev))
	}
	f.c.applied[e.Index] = cmd
	f.c.mu.Unlock()
	return e.Index
}

type fsmSnap struct {
	Last    uint64            `json:"last"`
	Applied map[uint64]string `json:"applied"`
}

func (f *testFSM) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return json.Marshal(fsmSnap{Last: f.last, Applied: f.applied})
}

func (f *testFSM) Restore(b []byte) error {
	var s fsmSnap
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.last = s.Last
	f.applied = s.Applied
	if f.applied == nil {
		f.applied = map[uint64]string{}
	}
	for idx, cmd := range s.Applied {
		f.c.mu.Lock()
		if prev, ok := f.c.applied[idx]; ok && prev != cmd {
			f.c.errs = append(f.c.errs, fmt.Sprintf("snapshot index %d: %q vs %q", idx, cmd, prev))
		}
		f.c.mu.Unlock()
	}
	return nil
}

func (c *cluster) fail(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, fmt.Sprintf(format, args...))
}

func others(ids []string, except ...string) []string {
	var out []string
	for _, id := range ids {
		skip := false
		for _, e := range except {
			skip = skip || id == e
		}
		if !skip {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

func isNotLeader(err error) bool {
	var nl *raft.NotLeaderError
	return errors.As(err, &nl)
}
