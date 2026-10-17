# 0003. JSON over HTTP for peer RPCs
Status: accepted
Date: 2026-10-17

## Context
Nodes exchange RequestVote, AppendEntries and InstallSnapshot. The project's goal is an auditable consensus core, not maximum throughput.

## Options
1. gRPC with protobuf. Compact, streaming for snapshots, generated clients. Adds code generation, a second listener or h2c, and makes traffic harder to inspect by hand.
2. JSON over HTTP/1.1 keep-alive on the same port as the client API. Debuggable with curl and tcpdump, no generated code, one listener. Larger messages and more CPU spent on encoding.

## Decision
Option 2. Being able to read every RPC during a failing chaos test is worth more here than encoding speed.

## Consequences
- The `raft.Transport` interface is the seam; a gRPC transport can replace it without touching the core.
- Snapshots go in one message, so very large states need a chunked InstallSnapshot. Listed under limitations.
- The peer port has no TLS or authentication. Run it on a private network or behind mTLS (see SECURITY.md).
