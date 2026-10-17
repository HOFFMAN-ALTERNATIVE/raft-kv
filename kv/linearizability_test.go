package kv_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/kv"
	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/raft"
	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/raft/memnet"
)

type kvCluster struct {
	ids    []string
	net    *memnet.Network
	stores map[string]*kv.Store
}

func newKVCluster(t *testing.T, n int) *kvCluster {
	t.Helper()
	c := &kvCluster{net: memnet.New(), stores: map[string]*kv.Store{}}
	for i := range n {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i+1))
	}
	for _, id := range c.ids {
		fsm := kv.NewFSM()
		node, err := raft.NewNode(raft.Config{
			ID: id, Peers: c.ids,
			ElectionTimeout: 150 * time.Millisecond, HeartbeatInterval: 30 * time.Millisecond,
			SnapshotThreshold: 50,
		}, c.net.Transport(id), raft.NewMemoryStorage(), fsm)
		if err != nil {
			t.Fatal(err)
		}
		c.net.Register(id, node)
		c.stores[id] = &kv.Store{Node: node, FSM: fsm}
	}
	t.Cleanup(func() {
		for _, s := range c.stores {
			s.Node.Stop()
		}
	})
	return c
}

// ---- porcupine model --------------------------------------------------

type opKind uint8

const (
	opGet opKind = iota
	opPut
	opCAS
)

type kvInput struct {
	Kind     opKind
	Key      string
	Value    string
	Expected *string
}

type kvOutput struct {
	Value string
	Found bool
	OK    bool
	// Unknown marks a write that never got a reply; CAS is deterministic
	// given the state, so the model only skips the output check.
	Unknown bool
}

type kvState struct {
	Value  string
	Exists bool
}

var kvModel = porcupine.Model{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range h {
			k := op.Input.(kvInput).Key
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(byKey))
		for _, ops := range byKey {
			out = append(out, ops)
		}
		return out
	},
	Init: func() any { return kvState{} },
	Step: func(state, input, output any) (bool, any) {
		s, in, out := state.(kvState), input.(kvInput), output.(kvOutput)
		switch in.Kind {
		case opGet:
			return out.Found == s.Exists && (!s.Exists || out.Value == s.Value), s
		case opPut:
			return true, kvState{Value: in.Value, Exists: true}
		default: // CAS
			match := (in.Expected == nil && !s.Exists) || (in.Expected != nil && s.Exists && *in.Expected == s.Value)
			if match {
				return out.OK || out.Unknown, kvState{Value: in.Value, Exists: true}
			}
			return !out.OK || out.Unknown, s
		}
	},
	DescribeOperation: func(input, output any) string {
		in, out := input.(kvInput), output.(kvOutput)
		switch in.Kind {
		case opGet:
			return fmt.Sprintf("get(%s) -> %q,%v", in.Key, out.Value, out.Found)
		case opPut:
			return fmt.Sprintf("put(%s, %s)", in.Key, in.Value)
		}
		return fmt.Sprintf("cas(%s, %v -> %s) -> %v", in.Key, in.Expected, in.Value, out.OK)
	},
}

// TestLinearizableUnderPartitions runs concurrent clients against a 5-node
// cluster on a lossy network with rolling partitions, records every
// operation's real-time interval, and asks porcupine whether some
// sequential order explains all observed results. Writes are retried with
// exactly-once sessions; reads go through ReadIndex.
func TestLinearizableUnderPartitions(t *testing.T) {
	if testing.Short() {
		t.Skip("linearizability check")
	}
	c := newKVCluster(t, 5)
	c.net.SetUnreliable(true)
	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }

	var (
		mu      sync.Mutex
		history []porcupine.Operation
		stop    atomic.Bool
		wg      sync.WaitGroup
	)
	keys := []string{"x", "y", "z"}
	const clients = 6
	for cl := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(cl), 99)) //nolint:gosec // test
			var seq uint64
			for i := 0; !stop.Load(); i++ {
				in := kvInput{Key: keys[rng.IntN(len(keys))], Value: fmt.Sprintf("c%d-%d", cl, i)}
				switch r := rng.IntN(10); {
				case r < 4:
					in.Kind = opGet
				case r < 8:
					in.Kind = opPut
				default:
					in.Kind = opCAS
					if rng.IntN(2) == 0 {
						e := fmt.Sprintf("c%d-%d", rng.IntN(clients), max(0, i-rng.IntN(3)))
						in.Expected = &e
					}
				}
				seq++
				call := now()
				out, ok := c.do(in, fmt.Sprintf("client-%d", cl), seq, &stop)
				if !ok {
					// Never completed: a write might still have happened,
					// so record it as pending until the end of time.
					if in.Kind != opGet {
						mu.Lock()
						history = append(history, porcupine.Operation{ClientId: cl, Input: in, Call: call, Output: kvOutput{Unknown: true}, Return: 1 << 62})
						mu.Unlock()
					}
					return
				}
				mu.Lock()
				history = append(history, porcupine.Operation{ClientId: cl, Input: in, Call: call, Output: out, Return: now()})
				mu.Unlock()
			}
		}()
	}

	rng := rand.New(rand.NewPCG(7, 7)) //nolint:gosec // test
	for time.Since(start) < 5*time.Second {
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
		time.Sleep(time.Duration(200+rng.IntN(400)) * time.Millisecond)
		c.net.Heal()
		time.Sleep(time.Duration(100+rng.IntN(300)) * time.Millisecond)
	}
	c.net.Heal()
	c.net.SetUnreliable(false)
	stop.Store(true)
	wg.Wait()

	res, info := porcupine.CheckOperationsVerbose(kvModel, history, 30*time.Second)
	t.Logf("checked %d operations: %s", len(history), res)
	if res == porcupine.Illegal {
		_ = porcupine.VisualizePath(kvModel, info, t.TempDir()+"/viz.html")
		t.Fatal("history is NOT linearizable")
	}
	if len(history) < 50 {
		t.Fatalf("too few operations completed (%d) — liveness problem", len(history))
	}
}

// do runs one client operation, retrying across nodes until it succeeds.
// Write retries reuse (clientID, seq) so they are applied at most once.
func (c *kvCluster) do(in kvInput, clientID string, seq uint64, stop *atomic.Bool) (kvOutput, bool) {
	deadline := time.Now().Add(20 * time.Second)
	for i := 0; time.Now().Before(deadline); i++ {
		s := c.stores[c.ids[i%len(c.ids)]]
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		var out kvOutput
		var err error
		if in.Kind == opGet {
			out.Value, out.Found, err = s.Get(ctx, in.Key, kv.Linearizable)
		} else {
			cmd := kv.Command{Op: kv.OpPut, Key: in.Key, Value: in.Value, ClientID: clientID, Seq: seq}
			if in.Kind == opCAS {
				cmd.Op, cmd.Expected = kv.OpCAS, in.Expected
			}
			var r kv.Result
			r, err = s.Apply(ctx, cmd)
			out.OK = r.OK
		}
		cancel()
		if err == nil {
			return out, true
		}
		var nl *raft.NotLeaderError
		if !errors.As(err, &nl) {
			time.Sleep(10 * time.Millisecond)
		}
		if stop.Load() && in.Kind == opGet {
			return out, false
		}
	}
	return kvOutput{}, false
}

// TestModelRejectsStaleRead is a negative control: the checker must flag a
// read that misses a write which completed before the read started.
func TestModelRejectsStaleRead(t *testing.T) {
	h := []porcupine.Operation{
		{ClientId: 0, Input: kvInput{Kind: opPut, Key: "x", Value: "1"}, Call: 0, Output: kvOutput{}, Return: 10},
		{ClientId: 1, Input: kvInput{Kind: opGet, Key: "x"}, Call: 20, Output: kvOutput{Found: false}, Return: 30},
	}
	if porcupine.CheckOperations(kvModel, h) {
		t.Fatal("stale read accepted as linearizable")
	}
	h[1].Output = kvOutput{Value: "1", Found: true}
	if !porcupine.CheckOperations(kvModel, h) {
		t.Fatal("correct history rejected")
	}
}
