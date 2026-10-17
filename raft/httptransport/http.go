// Package httptransport carries Raft RPCs as JSON over HTTP/1.1 keep-alive
// connections. It is deliberately boring: easy to inspect with curl/tcpdump,
// no code generation, and the same listener serves the client API.
package httptransport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/raft"
)

// Paths of the internal RPC endpoints.
const (
	PathVote     = "/raft/v1/request-vote"
	PathAppend   = "/raft/v1/append-entries"
	PathSnapshot = "/raft/v1/install-snapshot"
)

// maxBody bounds RPC bodies (snapshots are sent in one piece).
const maxBody = 256 << 20

// Transport implements raft.Transport over HTTP.
type Transport struct {
	peers  map[string]string // id -> base URL
	client *http.Client
}

// New returns a transport resolving peer ids through peers (id -> base URL).
func New(peers map[string]string) *Transport {
	return &Transport{
		peers: peers,
		client: &http.Client{Transport: &http.Transport{
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     90 * time.Second,
		}},
	}
}

func post[A, R any](ctx context.Context, t *Transport, peer, path string, args *A) (*R, error) {
	base, ok := t.peers[peer]
	if !ok {
		return nil, fmt.Errorf("httptransport: unknown peer %q", peer)
	}
	body, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck // read fully below
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("httptransport: %s %s: %s: %s", peer, path, resp.Status, bytes.TrimSpace(msg))
	}
	out := new(R)
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBody)).Decode(out); err != nil {
		return nil, err
	}
	return out, nil
}

// RequestVote implements raft.Transport.
func (t *Transport) RequestVote(ctx context.Context, peer string, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return post[raft.RequestVoteArgs, raft.RequestVoteReply](ctx, t, peer, PathVote, a)
}

// AppendEntries implements raft.Transport.
func (t *Transport) AppendEntries(ctx context.Context, peer string, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return post[raft.AppendEntriesArgs, raft.AppendEntriesReply](ctx, t, peer, PathAppend, a)
}

// InstallSnapshot implements raft.Transport.
func (t *Transport) InstallSnapshot(ctx context.Context, peer string, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return post[raft.InstallSnapshotArgs, raft.InstallSnapshotReply](ctx, t, peer, PathSnapshot, a)
}

// Register mounts the RPC handlers for node on mux.
func Register(mux *http.ServeMux, node *raft.Node) {
	mux.HandleFunc("POST "+PathVote, handle(node.HandleRequestVote))
	mux.HandleFunc("POST "+PathAppend, handle(node.HandleAppendEntries))
	mux.HandleFunc("POST "+PathSnapshot, handle(node.HandleInstallSnapshot))
}

func handle[A, R any](fn func(*A) (*R, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		args := new(A)
		if err := json.NewDecoder(io.LimitReader(r.Body, maxBody)).Decode(args); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		reply, err := fn(args)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(reply)
	}
}
