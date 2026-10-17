package raft_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/raft"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, 0)
	leader := c.checkOneLeader()
	term1 := c.term(leader)
	if term1 < 1 {
		t.Fatalf("term = %d, want >= 1", term1)
	}
	// No failures: the leader must keep its term (no spurious elections).
	time.Sleep(4 * electionTimeout)
	if l2 := c.checkOneLeader(); l2 != leader || c.term(l2) != term1 {
		t.Fatalf("leadership changed without failures: %s@%d -> %s@%d", leader, term1, l2, c.term(l2))
	}
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3, 0)
	l1 := c.checkOneLeader()

	c.net.Disconnect(l1)
	l2 := c.checkOneLeader(others(c.ids, l1)...)
	if l2 == l1 {
		t.Fatal("disconnected leader still leads the majority")
	}
	// CheckQuorum: the isolated old leader must step down on its own.
	time.Sleep(3 * electionTimeout)
	c.checkNoLeader(l1)

	// Pre-Vote: rejoining must not depose the healthy leader.
	termBefore := c.term(l2)
	c.net.Connect(l1)
	time.Sleep(4 * electionTimeout)
	if l := c.checkOneLeader(); l != l2 || c.term(l) != termBefore {
		t.Fatalf("rejoin disrupted the cluster: leader %s@%d -> %s@%d", l2, termBefore, l, c.term(l))
	}

	// Without a quorum nobody may lead.
	rest := others(c.ids, l2)
	c.net.Disconnect(l2)
	c.net.Disconnect(rest[0])
	time.Sleep(4 * electionTimeout)
	c.checkNoLeader(rest[1])

	c.net.Connect(rest[0])
	c.checkOneLeader(rest...)
	c.net.Connect(l2)
	c.checkOneLeader()
}

func TestPreVoteIsolatedFollowerDoesNotInflateTerm(t *testing.T) {
	c := newCluster(t, 3, 0)
	leader := c.checkOneLeader()
	f := others(c.ids, leader)[0]
	before := c.term(f)
	c.net.Disconnect(f)
	time.Sleep(10 * electionTimeout) // several election timeouts alone
	if got := c.term(f); got != before {
		t.Fatalf("isolated follower bumped its term %d -> %d (pre-vote should prevent this)", before, got)
	}
	c.net.Connect(f)
	time.Sleep(3 * electionTimeout)
	if l := c.checkOneLeader(); l != leader {
		t.Fatalf("leader changed %s -> %s after follower rejoined", leader, l)
	}
}

func TestBasicAgreement(t *testing.T) {
	c := newCluster(t, 5, 0)
	c.checkOneLeader()
	var last uint64
	for i := range 30 {
		idx := c.one(fmt.Sprintf("cmd-%d", i), 5)
		if idx <= last {
			t.Fatalf("index went backwards: %d after %d", idx, last)
		}
		last = idx
	}
}

func TestConcurrentProposals(t *testing.T) {
	c := newCluster(t, 3, 0)
	leader := c.node(c.checkOneLeader())
	var wg sync.WaitGroup
	idx := make([]uint64, 50)
	for i := range idx {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			res, err := leader.Propose(ctx, []byte(fmt.Sprintf("c%d", i)))
			if err != nil {
				t.Errorf("propose %d: %v", i, err)
				return
			}
			idx[i] = res.(uint64)
		}()
	}
	wg.Wait()
	seen := map[uint64]bool{}
	for i, x := range idx {
		if seen[x] {
			t.Fatalf("two proposals got index %d", x)
		}
		seen[x] = true
		waitCommitted(t, c, x, 3, fmt.Sprintf("c%d", i))
	}
}

func waitCommitted(t *testing.T, c *cluster, idx uint64, want int, cmd string) {
	t.Helper()
	for range 200 {
		if n, got := c.nCommitted(idx); n >= want {
			if got != cmd {
				t.Fatalf("index %d = %q, want %q", idx, got, cmd)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("index %d not applied on %d nodes", idx, want)
}

func TestFollowerFailureAndCatchUp(t *testing.T) {
	c := newCluster(t, 3, 0)
	leader := c.checkOneLeader()
	c.one("a", 3)
	f := others(c.ids, leader)[0]
	c.net.Disconnect(f)
	for i := range 10 {
		c.one(fmt.Sprintf("while-down-%d", i), 2)
	}
	c.net.Connect(f)
	idx := c.one("after", 3)
	waitCommitted(t, c, idx, 3, "after")
}

func TestNoCommitWithoutMajorityAndDivergentLogRepair(t *testing.T) {
	c := newCluster(t, 5, 0)
	c.one("base", 5)
	leader := c.checkOneLeader()
	minority := []string{leader, others(c.ids, leader)[0]}
	majority := others(c.ids, minority...)
	c.net.Partition(minority, majority)

	// The old leader accepts the proposal into its log, but it must never
	// commit: ctx expires or CheckQuorum demotes it first.
	ctx, cancel := context.WithTimeout(context.Background(), 4*electionTimeout)
	_, err := c.node(leader).Propose(ctx, []byte("doomed"))
	cancel()
	if err == nil {
		t.Fatal("minority leader committed an entry")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, raft.ErrLeadershipLost) && !isNotLeader(err) {
		t.Fatalf("unexpected error: %v", err)
	}

	newLeader := c.checkOneLeader(majority...)
	for i := range 5 {
		c.one(fmt.Sprintf("majority-%d", i), 3)
	}
	_ = newLeader

	c.net.Heal()
	idx := c.one("healed", 5)
	waitCommitted(t, c, idx, 5, "healed")
	for i := uint64(1); i <= idx; i++ {
		if _, cmd := c.nCommitted(i); cmd == "doomed" {
			t.Fatalf("uncommitted minority entry survived at index %d", i)
		}
	}
}

func TestPersistenceAcrossFullRestart(t *testing.T) {
	c := newCluster(t, 3, 0)
	for i := range 5 {
		c.one(fmt.Sprintf("pre-%d", i), 3)
	}
	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.start(id)
	}
	idx := c.one("post", 3)
	// Every restarted state machine replayed the full history.
	for i := uint64(1); i < idx; i++ {
		if n, cmd := c.nCommitted(i); n > 0 && n != 3 {
			t.Fatalf("index %d (%q) replayed on %d/3 nodes", i, cmd, n)
		}
	}
}

func TestLeaderCrashLoop(t *testing.T) {
	c := newCluster(t, 3, 0)
	c.one("start", 3)
	for i := range 4 {
		l := c.checkOneLeader()
		c.crash(l)
		c.one(fmt.Sprintf("after-crash-%d", i), 2)
		c.start(l)
	}
	c.one("end", 3)
}

func TestSnapshotInstallForLaggingFollower(t *testing.T) {
	const threshold = 10
	c := newCluster(t, 3, threshold)
	leader := c.checkOneLeader()
	lag := others(c.ids, leader)[0]
	c.net.Disconnect(lag)
	for i := range 60 {
		c.one(fmt.Sprintf("v%d", i), 2)
	}
	c.net.Connect(lag)
	idx := c.one("catch-up", 3)
	waitCommitted(t, c, idx, 3, "catch-up")

	st := c.node(lag).Status()
	if st.SnapshotsInstalled == 0 {
		t.Fatalf("lagging follower caught up without InstallSnapshot: %+v", st)
	}
	for _, id := range c.ids {
		st := c.node(id).Status()
		if inLog := st.LastLogIndex - st.SnapshotIndex; inLog > 2*threshold {
			t.Fatalf("%s keeps %d entries in its log; compaction not working", id, inLog)
		}
	}

	// A node restarting from a snapshot must restore the state machine.
	c.crash(lag)
	c.start(lag)
	idx = c.one("after-restart", 3)
	waitCommitted(t, c, idx, 3, "after-restart")
	if n, _ := c.nCommitted(5); n != 3 {
		t.Fatalf("restarted node lost snapshotted state (index 5 on %d/3)", n)
	}
}

func TestReadIndex(t *testing.T) {
	c := newCluster(t, 3, 0)
	leader := c.checkOneLeader()
	idx := c.one("x", 3)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ri, err := c.node(leader).ReadIndex(ctx)
	if err != nil || ri < idx {
		t.Fatalf("ReadIndex = %d, %v; want >= %d", ri, err, idx)
	}
	f := others(c.ids, leader)[0]
	if _, err := c.node(f).ReadIndex(ctx); !isNotLeader(err) {
		t.Fatalf("follower ReadIndex err = %v, want NotLeaderError", err)
	}

	// A leader cut off from the majority must refuse linearizable reads,
	// even before CheckQuorum demotes it: it cannot collect a quorum of acks.
	c.net.Disconnect(leader)
	ctx2, cancel2 := context.WithTimeout(context.Background(), electionTimeout/2)
	defer cancel2()
	if _, err := c.node(leader).ReadIndex(ctx2); err == nil {
		t.Fatal("partitioned leader served a linearizable read")
	}
}

// TestChaosUnreliableNetwork hammers a 5-node cluster with concurrent
// clients while randomly crashing, restarting and partitioning nodes on a
// lossy, reordering network. The global applied-index checker fails the test
// on any divergence; at the end everything heals and must converge.
func TestChaosUnreliableNetwork(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test")
	}
	c := newCluster(t, 5, 25)
	c.net.SetUnreliable(true)
	var stop atomic.Bool
	var wg sync.WaitGroup
	var acked atomic.Int64
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				for _, id := range c.ids {
					n := c.node(id)
					if n == nil {
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
					if _, err := n.Propose(ctx, []byte(fmt.Sprintf("w%d-%d", w, i))); err == nil {
						acked.Add(1)
					}
					cancel()
				}
			}
		}()
	}
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // test
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		switch rng.IntN(3) {
		case 0:
			id := c.ids[rng.IntN(len(c.ids))]
			if c.node(id) != nil {
				c.crash(id)
				time.Sleep(time.Duration(rng.IntN(300)) * time.Millisecond)
				c.start(id)
			}
		case 1:
			perm := rng.Perm(len(c.ids))
			cut := 1 + rng.IntN(len(c.ids)-1)
			var a, b []string
			for i, p := range perm {
				if i < cut {
					a = append(a, c.ids[p])
				} else {
					b = append(b, c.ids[p])
				}
			}
			c.net.Partition(a, b)
		default:
			c.net.Heal()
		}
		time.Sleep(time.Duration(100+rng.IntN(400)) * time.Millisecond)
	}
	stop.Store(true)
	wg.Wait()
	c.net.SetUnreliable(false)
	c.net.Heal()
	for _, id := range c.ids {
		if c.node(id) == nil {
			c.start(id)
		}
	}
	idx := c.one("final", 5)
	waitCommitted(t, c, idx, 5, "final")
	t.Logf("acknowledged writes during chaos: %d, rpcs: %d, final index: %d", acked.Load(), c.net.RPCCount(), idx)
	if acked.Load() == 0 {
		t.Fatal("no write made progress during chaos")
	}
}

func TestFileStoragePersists(t *testing.T) {
	dir := t.TempDir()
	s, err := raft.NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := s.LoadState(); b != nil {
		t.Fatal("fresh storage not empty")
	}
	if err := s.SaveState([]byte("state")); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveSnapshot([]byte("snap")); err != nil {
		t.Fatal(err)
	}
	s2, _ := raft.NewFileStorage(dir)
	st, _ := s2.LoadState()
	sn, _ := s2.LoadSnapshot()
	if string(st) != "state" || string(sn) != "snap" {
		t.Fatalf("got %q %q", st, sn)
	}
}

func TestConfigValidation(t *testing.T) {
	_, err := raft.NewNode(raft.Config{ID: "x", Peers: []string{"a"}}, nil, raft.NewMemoryStorage(), nil)
	if err == nil {
		t.Fatal("expected error for id not in peers")
	}
	_, err = raft.NewNode(raft.Config{ID: "a", Peers: []string{"a"}, ElectionTimeout: time.Millisecond, HeartbeatInterval: time.Second}, nil, raft.NewMemoryStorage(), nil)
	if err == nil {
		t.Fatal("expected error for heartbeat >= election timeout")
	}
}

func TestSingleNodeCluster(t *testing.T) {
	c := newCluster(t, 1, 5)
	c.checkOneLeader()
	for i := range 20 {
		c.one(fmt.Sprintf("solo-%d", i), 1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.node("n1").ReadIndex(ctx); err != nil {
		t.Fatal(err)
	}
}

// BenchmarkPropose3Nodes measures commit latency/throughput of sequential
// proposals through a 3-node in-memory cluster (each op = replicate to a
// majority + fsync-equivalent persist + apply).
func BenchmarkPropose3Nodes(b *testing.B) {
	c := newCluster(b, 3, 1000)
	leader := c.node(c.checkOneLeader())
	cmd := []byte("benchmark-payload-0123456789")
	b.ResetTimer()
	for range b.N {
		if _, err := leader.Propose(context.Background(), cmd); err != nil {
			b.Fatal(err)
		}
	}
}
