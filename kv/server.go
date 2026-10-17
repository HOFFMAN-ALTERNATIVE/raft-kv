package kv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/raft"
)

// maxValue bounds a single value.
const maxValue = 1 << 20

// Server is the client-facing HTTP API.
type Server struct {
	store   *Store
	peers   map[string]string // raft id -> base URL, for leader redirects
	timeout time.Duration
}

// NewServer returns an API server. peers maps node ids to their public base
// URLs so followers can redirect writes and linearizable reads to the leader.
func NewServer(store *Store, peers map[string]string) *Server {
	return &Server{store: store, peers: peers, timeout: 5 * time.Second}
}

// Register mounts the API on mux.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/kv/{key...}", s.get)
	mux.HandleFunc("PUT /v1/kv/{key...}", s.put)
	mux.HandleFunc("DELETE /v1/kv/{key...}", s.del)
	mux.HandleFunc("POST /v1/cas/{key...}", s.cas)
	mux.HandleFunc("GET /v1/keys", s.keys)
	mux.HandleFunc("GET /v1/status", s.status)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /readyz", s.ready)
	mux.HandleFunc("GET /metrics", s.metrics)
}

type kvResponse struct {
	Key     string `json:"key"`
	Value   string `json:"value,omitempty"`
	Found   bool   `json:"found"`
	Prev    string `json:"prev,omitempty"`
	Existed bool   `json:"existed,omitempty"`
	Index   uint64 `json:"index,omitempty"`
	OK      bool   `json:"ok"`
}

func (s *Server) ctx(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), s.timeout)
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.ctx(r)
	defer cancel()
	c := Linearizable
	if r.URL.Query().Get("consistency") == "stale" {
		c = Stale
	}
	key := r.PathValue("key")
	v, ok, err := s.store.Get(ctx, key, c)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	code := http.StatusOK
	if !ok {
		code = http.StatusNotFound
	}
	writeJSON(w, code, kvResponse{Key: key, Value: v, Found: ok, OK: ok})
}

func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxValue))
	if err != nil {
		http.Error(w, "value too large", http.StatusRequestEntityTooLarge)
		return
	}
	s.apply(w, r, Command{Op: OpPut, Key: r.PathValue("key"), Value: string(body)})
}

func (s *Server) del(w http.ResponseWriter, r *http.Request) {
	s.apply(w, r, Command{Op: OpDelete, Key: r.PathValue("key")})
}

type casRequest struct {
	Expected *string `json:"expected"`
	Value    string  `json:"value"`
}

func (s *Server) cas(w http.ResponseWriter, r *http.Request) {
	var req casRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxValue)).Decode(&req); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	s.apply(w, r, Command{Op: OpCAS, Key: r.PathValue("key"), Value: req.Value, Expected: req.Expected})
}

func (s *Server) apply(w http.ResponseWriter, r *http.Request, c Command) {
	if c.Key == "" {
		http.Error(w, "empty key", http.StatusBadRequest)
		return
	}
	// Optional exactly-once session headers for safe client retries.
	if id := r.Header.Get("X-Client-ID"); id != "" {
		seq, err := strconv.ParseUint(r.Header.Get("X-Request-Seq"), 10, 64)
		if err != nil || seq == 0 {
			http.Error(w, "X-Request-Seq must be a positive integer when X-Client-ID is set", http.StatusBadRequest)
			return
		}
		c.ClientID, c.Seq = id, seq
	}
	ctx, cancel := s.ctx(r)
	defer cancel()
	res, err := s.store.Apply(ctx, c)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	code := http.StatusOK
	if c.Op == OpCAS && !res.OK {
		code = http.StatusConflict
	}
	if c.Op == OpDelete && !res.OK {
		code = http.StatusNotFound
	}
	writeJSON(w, code, kvResponse{Key: c.Key, Value: c.Value, Prev: res.Prev, Existed: res.Existed, Index: res.Index, OK: res.OK, Found: res.Existed})
}

// fail maps Raft errors onto HTTP semantics: followers answer with a 307 to
// the leader (curl -L and most clients follow it, keeping method and body).
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	var nl *raft.NotLeaderError
	switch {
	case errors.As(err, &nl) && s.peers[nl.Leader] != "":
		w.Header().Set("Location", s.peers[nl.Leader]+r.URL.RequestURI())
		w.Header().Set("X-Raft-Leader", nl.Leader)
		writeJSON(w, http.StatusTemporaryRedirect, map[string]string{"error": err.Error(), "leader": nl.Leader})
	case errors.As(err, &nl):
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no leader elected yet"})
	case errors.Is(err, raft.ErrLeadershipLost), errors.Is(err, context.DeadlineExceeded):
		// The write may or may not have been applied; retry with the same
		// X-Client-ID/X-Request-Seq to stay exactly-once.
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error(), "outcome": "unknown"})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
}

func (s *Server) keys(w http.ResponseWriter, r *http.Request) {
	limit := 1000
	if l, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && l > 0 && l < limit {
		limit = l
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": s.store.FSM.Keys(r.URL.Query().Get("prefix"), limit)})
}

func (s *Server) status(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.store.Node.Status())
}

// ready reports 200 once the node knows a leader (itself or another).
func (s *Server) ready(w http.ResponseWriter, _ *http.Request) {
	st := s.store.Node.Status()
	if st.Leader == "" {
		writeJSON(w, http.StatusServiceUnavailable, st)
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	st := s.store.Node.Status()
	var isLeader uint64
	if st.Role == "leader" {
		isLeader = 1
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, m := range []struct {
		name, help, typ string
		v               uint64
	}{
		{"raft_term", "Current Raft term.", "gauge", st.Term},
		{"raft_is_leader", "1 if this node is the leader.", "gauge", isLeader},
		{"raft_commit_index", "Highest committed log index.", "gauge", st.CommitIndex},
		{"raft_last_applied", "Highest applied log index.", "gauge", st.LastApplied},
		{"raft_last_log_index", "Index of the last log entry.", "gauge", st.LastLogIndex},
		{"raft_snapshot_index", "Last index covered by the snapshot.", "gauge", st.SnapshotIndex},
		{"raft_snapshots_taken_total", "Snapshots taken locally.", "counter", st.SnapshotsTaken},
		{"raft_snapshots_installed_total", "Snapshots installed from the leader.", "counter", st.SnapshotsInstalled},
		{"raft_elections_started_total", "Elections started by this node.", "counter", st.Elections},
		{"kv_keys", "Number of keys in the store.", "gauge", uint64(s.store.FSM.Len())}, //nolint:gosec // len is never negative
	} {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s{node=%q} %d\n", m.name, m.help, m.name, m.typ, m.name, st.ID, m.v)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
