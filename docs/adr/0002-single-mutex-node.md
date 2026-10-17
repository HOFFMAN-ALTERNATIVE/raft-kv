# 0002. One mutex per node instead of an actor loop
Status: accepted
Date: 2026-10-17

## Context
Raft state (term, vote, log, commit index, role) is one set of tightly coupled invariants. Go offers two common shapes: a single goroutine owning the state and receiving messages on channels, or shared state behind a lock.

## Options
1. Actor loop with channels. No locks in user code. In practice needs a dozen channels, select loops with priority problems, and invariants spread across message handlers; deadlocks show up as stuck channels that `-race` cannot see.
2. One mutex plus condition variables, with blocking work (RPCs, FSM apply, snapshot fsync) always done with the lock released. Invariants are checked in one place; the race detector covers every access.

## Decision
Option 2. Correctness reviews are easier when every state transition happens under one lock in straight-line code.

## Consequences
- Three goroutine kinds (ticker, one replicator per peer, applier) share the lock; only the applier touches the state machine.
- Throughput is bounded by lock contention on very large clusters or very high proposal rates. Not a concern at 3-5 nodes; batching proposals is the first fix if it becomes one.
- Every code path that does I/O must drop the lock first; review checks this explicitly.
