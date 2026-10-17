package kv_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/kv"
	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/raft"
	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/raft/httptransport"
)

// httpCluster runs real HTTP servers: Raft RPCs and the client API share a
// listener, exactly as in the binary.
type httpCluster struct {
	urls   map[string]string
	nodes  map[string]*raft.Node
	srvs   []*httptest.Server
	client *http.Client
}

func newHTTPCluster(t *testing.T) *httpCluster {
	t.Helper()
	ids := []string{"n1", "n2", "n3"}
	hc := &httpCluster{urls: map[string]string{}, nodes: map[string]*raft.Node{}, client: &http.Client{Timeout: 10 * time.Second}}
	muxes := map[string]*http.ServeMux{}
	for _, id := range ids {
		mux := http.NewServeMux()
		srv := httptest.NewUnstartedServer(mux)
		muxes[id] = mux
		hc.srvs = append(hc.srvs, srv)
		hc.urls[id] = "http://" + srv.Listener.Addr().String()
	}
	for i, id := range ids {
		fsm := kv.NewFSM()
		node, err := raft.NewNode(raft.Config{
			ID: id, Peers: ids, ElectionTimeout: 150 * time.Millisecond, SnapshotThreshold: 20,
		}, httptransport.New(hc.urls), raft.NewMemoryStorage(), fsm)
		if err != nil {
			t.Fatal(err)
		}
		hc.nodes[id] = node
		httptransport.Register(muxes[id], node)
		kv.NewServer(&kv.Store{Node: node, FSM: fsm}, hc.urls).Register(muxes[id])
		hc.srvs[i].Start()
	}
	t.Cleanup(func() {
		for _, n := range hc.nodes {
			n.Stop()
		}
		for _, s := range hc.srvs {
			s.Close()
		}
	})
	return hc
}

func (hc *httpCluster) leader(t *testing.T) string {
	t.Helper()
	for range 100 {
		for id, n := range hc.nodes {
			if _, ok := n.State(); ok {
				return id
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("no leader")
	return ""
}

func (hc *httpCluster) do(t *testing.T, method, url, body string, hdr ...string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := hc.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return resp.StatusCode, out
}

func TestHTTPAPIEndToEnd(t *testing.T) {
	hc := newHTTPCluster(t)
	leader := hc.leader(t)
	var follower string
	for id := range hc.nodes {
		if id != leader {
			follower = id
			break
		}
	}
	fURL, lURL := hc.urls[follower], hc.urls[leader]

	// Writes to a follower are redirected (307) to the leader; Go's client
	// follows 307 for PUT and replays the body.
	code, out := hc.do(t, http.MethodPut, fURL+"/v1/kv/greeting", "hello")
	if code != http.StatusOK || out["ok"] != true {
		t.Fatalf("PUT via follower: %d %v", code, out)
	}
	code, out = hc.do(t, http.MethodGet, lURL+"/v1/kv/greeting", "")
	if code != http.StatusOK || out["value"] != "hello" {
		t.Fatalf("linearizable GET: %d %v", code, out)
	}
	// Stale reads are served locally and converge.
	var stale map[string]any
	for range 50 {
		_, stale = hc.do(t, http.MethodGet, fURL+"/v1/kv/greeting?consistency=stale", "")
		if stale["value"] == "hello" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if stale["value"] != "hello" {
		t.Fatalf("stale read never converged: %v", stale)
	}

	// Nested keys, CAS, delete.
	if code, _ := hc.do(t, http.MethodPut, lURL+"/v1/kv/app/config/a", "1"); code != 200 {
		t.Fatalf("nested put: %d", code)
	}
	if code, _ := hc.do(t, http.MethodPost, lURL+"/v1/cas/app/config/a", `{"expected":"0","value":"2"}`); code != http.StatusConflict {
		t.Fatalf("cas mismatch: %d", code)
	}
	if code, _ := hc.do(t, http.MethodPost, lURL+"/v1/cas/app/config/a", `{"expected":"1","value":"2"}`); code != 200 {
		t.Fatalf("cas match: %d", code)
	}
	if code, _ := hc.do(t, http.MethodPost, lURL+"/v1/cas/x", `not json`); code != http.StatusBadRequest {
		t.Fatalf("cas bad body: %d", code)
	}
	code, out = hc.do(t, http.MethodGet, lURL+"/v1/keys?prefix=app/", "")
	if code != 200 || fmt.Sprint(out["keys"]) != "[app/config/a]" {
		t.Fatalf("keys: %d %v", code, out)
	}
	if code, _ := hc.do(t, http.MethodDelete, lURL+"/v1/kv/app/config/a", ""); code != 200 {
		t.Fatalf("delete: %d", code)
	}
	if code, _ := hc.do(t, http.MethodDelete, lURL+"/v1/kv/app/config/a", ""); code != http.StatusNotFound {
		t.Fatalf("delete missing: %d", code)
	}
	if code, _ := hc.do(t, http.MethodGet, lURL+"/v1/kv/app/config/a", ""); code != http.StatusNotFound {
		t.Fatalf("get deleted: %d", code)
	}

	// Exactly-once sessions over HTTP.
	if code, _ := hc.do(t, http.MethodPut, lURL+"/v1/kv/s", "v", "X-Client-ID", "c"); code != http.StatusBadRequest {
		t.Fatalf("missing seq accepted: %d", code)
	}
	_, first := hc.do(t, http.MethodPost, lURL+"/v1/cas/once", `{"value":"1"}`, "X-Client-ID", "c", "X-Request-Seq", "1")
	_, retry := hc.do(t, http.MethodPost, lURL+"/v1/cas/once", `{"value":"1"}`, "X-Client-ID", "c", "X-Request-Seq", "1")
	if first["ok"] != true || retry["ok"] != true || first["index"] != retry["index"] {
		t.Fatalf("retry not deduplicated: %v vs %v", first, retry)
	}

	// Enough writes to trigger snapshots, then status + metrics.
	for i := range 30 {
		hc.do(t, http.MethodPut, lURL+fmt.Sprintf("/v1/kv/bulk/%d", i), "x")
	}
	code, out = hc.do(t, http.MethodGet, lURL+"/v1/status", "")
	if code != 200 || out["role"] != "leader" || out["snapshots_taken"].(float64) < 1 {
		t.Fatalf("status: %d %v", code, out)
	}
	resp, err := hc.client.Get(lURL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	m, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(m), `raft_is_leader{node="`+leader+`"} 1`) {
		t.Fatalf("metrics missing leader gauge:\n%s", m)
	}
	if code, _ := hc.do(t, http.MethodGet, fURL+"/readyz", ""); code != 200 {
		t.Fatalf("readyz: %d", code)
	}
}

func TestHTTPNoLeaderIs503(t *testing.T) {
	mux := http.NewServeMux()
	srv := httptest.NewServer(mux)
	defer srv.Close()
	fsm := kv.NewFSM()
	// Two-member config where the peer never answers: no leader can exist.
	node, err := raft.NewNode(raft.Config{ID: "a", Peers: []string{"a", "b"}, ElectionTimeout: 100 * time.Millisecond},
		httptransport.New(map[string]string{"b": "http://127.0.0.1:1"}), raft.NewMemoryStorage(), fsm)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()
	kv.NewServer(&kv.Store{Node: node, FSM: fsm}, nil).Register(mux)
	resp, err := http.Post(srv.URL+"/v1/cas/k", "application/json", strings.NewReader(`{"value":"v"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("got %d, want 503 with Retry-After", resp.StatusCode)
	}
	resp, _ = http.Get(srv.URL + "/readyz")
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz = %d", resp.StatusCode)
	}
}
