# Security

## Reporting

Report vulnerabilities privately through GitHub's "Report a vulnerability" button on this repository (Security tab). I aim to reply within 72 hours. Please do not open public issues for exploitable bugs.

## Threat model

- **Assets:** the replicated data and its consistency; availability of the cluster.
- **Actors:** clients of the KV API; anyone with network access to a node's port.
- **Entry points:** client API (`/v1/*`), peer RPCs (`/raft/v1/*`), `/metrics`, flags and environment.
- **Top risks and mitigations:**
  - Oversized requests: values capped at 1 MiB, RPC bodies capped in the transport, HTTP read/write timeouts.
  - Duplicate writes on retry: client sessions make retries exactly-once.
  - Data loss on crash: write-temp, fsync, rename, fsync(dir); the node panics rather than continue after a failed persist.
  - Container escape surface: distroless, non-root, read-only filesystem, all capabilities dropped (compose).

## Not provided

There is no authentication or TLS on either the client API or the peer port. Anyone who can reach a peer port can forge Raft messages and corrupt the cluster. Run nodes on a private network, behind mTLS (for example a service mesh), and never expose port 7001 publicly. The compose file binds host ports to 127.0.0.1 for that reason.
