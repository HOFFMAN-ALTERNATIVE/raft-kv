// Package kv is a replicated, linearizable key-value store built on the raft
// package: a deterministic state machine with exactly-once client sessions,
// a Store facade for reads/writes and an HTTP API.
package kv

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/raft"
)

// Op is a mutating operation.
type Op string

// Supported operations.
const (
	OpPut    Op = "put"
	OpDelete Op = "delete"
	OpCAS    Op = "cas"
)

// Command is the replicated log payload.
type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
	// Expected is the required current value for OpCAS; nil means "key must
	// not exist".
	Expected *string `json:"expected,omitempty"`
	// ClientID and Seq form an exactly-once session: a retried command with
	// a Seq the state machine has already applied for ClientID returns the
	// cached result instead of being applied twice (thesis §6.3).
	ClientID string `json:"client_id,omitempty"`
	Seq      uint64 `json:"seq,omitempty"`
}

// Result is what applying a Command produced.
type Result struct {
	OK      bool   `json:"ok"`
	Prev    string `json:"prev,omitempty"`
	Existed bool   `json:"existed"`
	Index   uint64 `json:"index"`
	Err     string `json:"error,omitempty"`
}

type session struct {
	Seq    uint64 `json:"seq"`
	Result Result `json:"result"`
}

// FSM is the key-value state machine. Apply/Snapshot/Restore are called by
// the Raft applier goroutine; Get may be called concurrently.
type FSM struct {
	mu       sync.RWMutex
	data     map[string]string
	sessions map[string]session
}

// NewFSM returns an empty state machine.
func NewFSM() *FSM {
	return &FSM{data: map[string]string{}, sessions: map[string]session{}}
}

var _ raft.StateMachine = (*FSM)(nil)

// Apply implements raft.StateMachine.
func (f *FSM) Apply(e raft.Entry) any {
	var c Command
	if err := json.Unmarshal(e.Data, &c); err != nil {
		return Result{Index: e.Index, Err: fmt.Sprintf("decode: %v", err)}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if c.ClientID != "" {
		if s, ok := f.sessions[c.ClientID]; ok && c.Seq <= s.Seq {
			return s.Result // duplicate delivery of a retried request
		}
	}
	prev, existed := f.data[c.Key]
	r := Result{Index: e.Index, Prev: prev, Existed: existed}
	switch c.Op {
	case OpPut:
		f.data[c.Key] = c.Value
		r.OK = true
	case OpDelete:
		delete(f.data, c.Key)
		r.OK = existed
	case OpCAS:
		if (c.Expected == nil && !existed) || (c.Expected != nil && existed && *c.Expected == prev) {
			f.data[c.Key] = c.Value
			r.OK = true
		}
	default:
		r.Err = fmt.Sprintf("unknown op %q", c.Op)
	}
	if c.ClientID != "" {
		f.sessions[c.ClientID] = session{Seq: c.Seq, Result: r}
	}
	return r
}

// Get reads local state. Callers wanting linearizability must go through
// Store.Get, which runs ReadIndex first.
func (f *FSM) Get(key string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	v, ok := f.data[key]
	return v, ok
}

// Keys returns up to limit keys with the given prefix, sorted.
func (f *FSM) Keys(prefix string, limit int) []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	keys := make([]string, 0, min(limit, len(f.data)))
	for k := range f.data {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	return keys
}

// Len returns the number of keys.
func (f *FSM) Len() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.data)
}

type snapshot struct {
	Data     map[string]string  `json:"data"`
	Sessions map[string]session `json:"sessions"`
}

// Snapshot implements raft.StateMachine. Sessions are part of the snapshot:
// dropping them would let a retry after a restore apply twice.
func (f *FSM) Snapshot() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return json.Marshal(snapshot{Data: f.data, Sessions: f.sessions})
}

// Restore implements raft.StateMachine.
func (f *FSM) Restore(b []byte) error {
	var s snapshot
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	if s.Data == nil {
		s.Data = map[string]string{}
	}
	if s.Sessions == nil {
		s.Sessions = map[string]session{}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data, f.sessions = s.Data, s.Sessions
	return nil
}
