// Package memnet is a fault-injecting in-memory Transport for testing Raft
// clusters deterministically fast: it can disconnect nodes, split the
// cluster into arbitrary partitions, drop or delay messages and reorder
// replies — every message is deep-copied through JSON, so nodes never share
// memory and serialisation bugs surface in tests.
package memnet

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/raft"
)

// Handler is the server side of the Raft RPCs (implemented by *raft.Node).
type Handler interface {
	HandleRequestVote(*raft.RequestVoteArgs) (*raft.RequestVoteReply, error)
	HandleAppendEntries(*raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error)
	HandleInstallSnapshot(*raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error)
}

// ErrUnreachable is returned for messages that the network dropped.
var ErrUnreachable = errors.New("memnet: unreachable")

// Network connects named endpoints.
type Network struct {
	mu         sync.RWMutex
	handlers   map[string]Handler
	connected  map[string]bool
	group      map[string]int // partition id; only equal ids can talk
	unreliable bool
	longDelays bool
	rpcs       atomic.Int64
}

// New returns an empty, reliable network.
func New() *Network {
	return &Network{handlers: map[string]Handler{}, connected: map[string]bool{}, group: map[string]int{}}
}

// Register (re)binds id to h and connects it.
func (nw *Network) Register(id string, h Handler) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.handlers[id] = h
	nw.connected[id] = true
}

// Connect re-attaches id to the network.
func (nw *Network) Connect(id string) { nw.setConnected(id, true) }

// Disconnect isolates id: it can neither send nor receive.
func (nw *Network) Disconnect(id string) { nw.setConnected(id, false) }

func (nw *Network) setConnected(id string, v bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.connected[id] = v
}

// Partition splits the network: nodes in the same group can talk, nodes in
// different groups cannot. Nodes not listed land in group 0.
func (nw *Network) Partition(groups ...[]string) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.group = map[string]int{}
	for i, g := range groups {
		for _, id := range g {
			nw.group[id] = i + 1
		}
	}
}

// Heal removes all partitions and reconnects every node.
func (nw *Network) Heal() {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.group = map[string]int{}
	for id := range nw.connected {
		nw.connected[id] = true
	}
}

// SetUnreliable enables random drops (10% of requests and of replies) and
// random delays up to 27ms.
func (nw *Network) SetUnreliable(v bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.unreliable = v
}

// SetLongDelays makes messages to unreachable peers hang for a long time
// instead of failing fast, like a black-holed TCP connection.
func (nw *Network) SetLongDelays(v bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.longDelays = v
}

// RPCCount returns the number of RPCs attempted so far.
func (nw *Network) RPCCount() int64 { return nw.rpcs.Load() }

func (nw *Network) reachable(from, to string) (h Handler, ok, unreliable, long bool) {
	nw.mu.RLock()
	defer nw.mu.RUnlock()
	h = nw.handlers[to]
	ok = h != nil && nw.connected[from] && nw.connected[to] && nw.group[from] == nw.group[to]
	return h, ok, nw.unreliable, nw.longDelays
}

// Transport returns the client side of the network for node from.
func (nw *Network) Transport(from string) raft.Transport { return &transport{nw: nw, from: from} }

type transport struct {
	nw   *Network
	from string
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func call[A, R any](ctx context.Context, t *transport, to string, args *A, fn func(Handler, *A) (*R, error)) (*R, error) {
	t.nw.rpcs.Add(1)
	h, ok, unreliable, long := t.nw.reachable(t.from, to)
	if !ok {
		d := time.Duration(rand.N(10)) * time.Millisecond //nolint:gosec // test jitter
		if long {
			d = time.Duration(rand.N(2000)) * time.Millisecond //nolint:gosec // test jitter
		}
		_ = sleep(ctx, d)
		return nil, ErrUnreachable
	}
	if unreliable {
		if err := sleep(ctx, time.Duration(rand.N(27))*time.Millisecond); err != nil { //nolint:gosec // test jitter
			return nil, err
		}
		if rand.N(10) == 0 { //nolint:gosec // test jitter
			return nil, ErrUnreachable
		}
	}
	in, err := roundTrip(args)
	if err != nil {
		return nil, err
	}
	reply, err := fn(h, in)
	if err != nil {
		return nil, err
	}
	// The reply travels back over the same, possibly changed, network.
	if _, ok, unreliable, _ := t.nw.reachable(t.from, to); !ok || (unreliable && rand.N(10) == 0) { //nolint:gosec // test jitter
		return nil, ErrUnreachable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return roundTrip(reply)
}

func roundTrip[T any](v *T) (*T, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	out := new(T)
	return out, json.Unmarshal(b, out)
}

func (t *transport) RequestVote(ctx context.Context, peer string, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return call(ctx, t, peer, a, Handler.HandleRequestVote)
}

func (t *transport) AppendEntries(ctx context.Context, peer string, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return call(ctx, t, peer, a, Handler.HandleAppendEntries)
}

func (t *transport) InstallSnapshot(ctx context.Context, peer string, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return call(ctx, t, peer, a, Handler.HandleInstallSnapshot)
}
