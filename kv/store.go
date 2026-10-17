package kv

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/raft"
)

// Consistency selects the read path.
type Consistency int

const (
	// Linearizable reads go through ReadIndex on the leader.
	Linearizable Consistency = iota
	// Stale reads serve local state on any node; they may lag the leader.
	Stale
)

// Store couples a Raft node with the KV state machine.
type Store struct {
	Node *raft.Node
	FSM  *FSM
}

// Apply proposes c and returns the state machine's result.
func (s *Store) Apply(ctx context.Context, c Command) (Result, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return Result{}, err
	}
	v, err := s.Node.Propose(ctx, b)
	if err != nil {
		return Result{}, err
	}
	r, ok := v.(Result)
	if !ok {
		return Result{}, fmt.Errorf("kv: unexpected apply result %T", v)
	}
	if r.Err != "" {
		return r, fmt.Errorf("kv: %s", r.Err)
	}
	return r, nil
}

// Get reads key with the requested consistency.
func (s *Store) Get(ctx context.Context, key string, c Consistency) (string, bool, error) {
	if c == Linearizable {
		if _, err := s.Node.ReadIndex(ctx); err != nil {
			return "", false, err
		}
	}
	v, ok := s.FSM.Get(key)
	return v, ok, nil
}
