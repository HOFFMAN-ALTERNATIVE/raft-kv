// Package raft is a from-scratch implementation of the Raft consensus
// algorithm (Ongaro & Ousterhout, 2014) with the extensions a production
// deployment needs: Pre-Vote, CheckQuorum, ReadIndex linearizable reads,
// log compaction via snapshots and InstallSnapshot for lagging followers.
//
// The package is transport- and storage-agnostic: a Node talks to its peers
// through a Transport and persists state through a Storage, so the same core
// runs over HTTP in production and over a fault-injecting in-memory network
// in tests.
package raft

import (
	"errors"
	"fmt"
)

// Role is the current role of a node.
type Role uint8

// Raft roles. PreCandidate is the Pre-Vote phase (thesis §9.6): the node
// asks whether it *could* win an election before disrupting the cluster by
// bumping its term.
const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return fmt.Sprintf("role(%d)", uint8(r))
}

// EntryType distinguishes client commands from internal entries.
type EntryType uint8

const (
	// EntryCommand carries an opaque state-machine command.
	EntryCommand EntryType = iota
	// EntryNoop is appended by every new leader so that it can commit an
	// entry from its own term (§5.4.2) and serve ReadIndex requests.
	EntryNoop
)

// Entry is a single replicated log entry.
type Entry struct {
	Index uint64    `json:"index"`
	Term  uint64    `json:"term"`
	Type  EntryType `json:"type"`
	Data  []byte    `json:"data,omitempty"`
}

// RequestVoteArgs is sent by (pre-)candidates.
type RequestVoteArgs struct {
	Term         uint64 `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
	// PreVote marks a non-binding straw poll: receivers answer as if Term
	// were real but change no state.
	PreVote bool `json:"pre_vote,omitempty"`
}

// RequestVoteReply answers RequestVoteArgs.
type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntriesArgs replicates entries and doubles as the heartbeat.
type AppendEntriesArgs struct {
	Term         uint64  `json:"term"`
	LeaderID     string  `json:"leader_id"`
	PrevLogIndex uint64  `json:"prev_log_index"`
	PrevLogTerm  uint64  `json:"prev_log_term"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"leader_commit"`
}

// AppendEntriesReply answers AppendEntriesArgs. On a consistency-check
// failure the follower returns ConflictTerm/ConflictIndex so that the leader
// can skip a whole term per round trip instead of one entry.
type AppendEntriesReply struct {
	Term          uint64 `json:"term"`
	Success       bool   `json:"success"`
	ConflictTerm  uint64 `json:"conflict_term,omitempty"`
	ConflictIndex uint64 `json:"conflict_index,omitempty"`
}

// InstallSnapshotArgs ships a full state-machine snapshot to a follower whose
// next entry has already been compacted away on the leader.
type InstallSnapshotArgs struct {
	Term              uint64 `json:"term"`
	LeaderID          string `json:"leader_id"`
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
	Data              []byte `json:"data"`
}

// InstallSnapshotReply answers InstallSnapshotArgs.
type InstallSnapshotReply struct {
	Term uint64 `json:"term"`
}

// StateMachine is the replicated application. Apply is called exactly once
// per committed command entry, in log order, from a single goroutine.
// Snapshot and Restore are called from that same goroutine, so an
// implementation only needs to guard against its own concurrent readers.
type StateMachine interface {
	Apply(e Entry) any
	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error
}

// Errors returned by Node.
var (
	ErrStopped        = errors.New("raft: node stopped")
	ErrLeadershipLost = errors.New("raft: leadership lost before the entry was applied; outcome unknown")
)

// NotLeaderError is returned when a request that requires the leader hits a
// follower. Leader is the last known leader id, or "" if unknown.
type NotLeaderError struct{ Leader string }

func (e *NotLeaderError) Error() string {
	if e.Leader == "" {
		return "raft: not the leader (leader unknown)"
	}
	return "raft: not the leader (leader is " + e.Leader + ")"
}

// Status is a point-in-time view of a node, for APIs and metrics.
type Status struct {
	ID                 string `json:"id"`
	Role               string `json:"role"`
	Term               uint64 `json:"term"`
	Leader             string `json:"leader"`
	CommitIndex        uint64 `json:"commit_index"`
	LastApplied        uint64 `json:"last_applied"`
	LastLogIndex       uint64 `json:"last_log_index"`
	SnapshotIndex      uint64 `json:"snapshot_index"`
	SnapshotsTaken     uint64 `json:"snapshots_taken"`
	SnapshotsInstalled uint64 `json:"snapshots_installed"`
	Elections          uint64 `json:"elections_started"`
}
