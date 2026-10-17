# 0001. ReadIndex for linearizable reads
Status: accepted
Date: 2026-10-17

## Context
A leader that has been partitioned away may not know it was replaced. If it answers reads from local state it can return stale data, which breaks linearizability. Reads are usually the majority of KV traffic, so their cost matters.

## Options
1. Reads through the log. Trivially correct. Every read costs a log append, an fsync and a replication round.
2. Leader leases. Reads are local and cheap. Correctness depends on bounded clock drift between nodes; a paused VM or a clock jump breaks it silently.
3. ReadIndex (thesis §6.4). The leader records its commit index, confirms leadership with one heartbeat round, waits until it has applied that index, then reads locally. No disk write, no clock assumption.

## Decision
Option 3. It relies on nothing but quorum, which is the assumption Raft already makes, and it costs one network round instead of a disk write.

## Consequences
- Heartbeats carry a sequence number, so one round confirms many concurrent reads.
- Every new leader appends a no-op first, otherwise it cannot know the true commit index.
- Followers redirect linearizable reads to the leader. `?consistency=stale` exists for callers who accept lag.
- `TestLinearizableUnderPartitions` plus a negative control (`TestModelRejectsStaleRead`) keep this honest. Revisit leases only if read latency becomes the bottleneck and clock behaviour is controlled.
