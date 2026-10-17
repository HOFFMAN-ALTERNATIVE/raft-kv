// Command raftkv runs one node of a replicated key-value store.
//
//	raftkv -id n1 -listen :7001 -data /var/lib/raftkv \
//	  -peers n1=http://n1:7001,n2=http://n2:7001,n3=http://n3:7001
//
// -peers are the addresses nodes use to reach each other; -client-urls
// (optional) are the addresses clients should be redirected to, for when
// the two differ (NAT, docker port mapping, load balancers).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/kv"
	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/raft"
	"github.com/HOFFMAN-ALTERNATIVE/raft-kv/raft/httptransport"
)

var version = "dev"

func main() {
	// `raftkv healthcheck` probes the local node so the distroless image
	// (no shell, no curl) can still run a Docker HEALTHCHECK.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		if err := healthcheck(envOr("RAFTKV_LISTEN", ":7001")); err != nil {
			fmt.Fprintln(os.Stderr, "unhealthy:", err)
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "raftkv:", err)
		os.Exit(1)
	}
}

func parsePeers(s string) (map[string]string, []string, error) {
	parts := strings.Split(s, ",")
	peers := make(map[string]string, len(parts))
	ids := make([]string, 0, len(parts))
	for _, p := range parts {
		id, url, ok := strings.Cut(strings.TrimSpace(p), "=")
		if !ok || id == "" || url == "" {
			return nil, nil, fmt.Errorf("bad peer %q, want id=url", p)
		}
		peers[id] = strings.TrimRight(url, "/")
		ids = append(ids, id)
	}
	return peers, ids, nil
}

func run() error {
	var (
		id         = flag.String("id", os.Getenv("RAFTKV_ID"), "node id (must appear in -peers)")
		listen     = flag.String("listen", envOr("RAFTKV_LISTEN", ":7001"), "listen address for client API and raft RPCs")
		peersFlag  = flag.String("peers", os.Getenv("RAFTKV_PEERS"), "comma-separated id=url list of all members")
		clientFlag = flag.String("client-urls", os.Getenv("RAFTKV_CLIENT_URLS"), "id=url list advertised to clients in leader redirects (default: -peers)")
		dataDir    = flag.String("data", envOr("RAFTKV_DATA", "./data"), "data directory")
		election   = flag.Duration("election-timeout", 500*time.Millisecond, "minimum election timeout")
		heartbeat  = flag.Duration("heartbeat", 100*time.Millisecond, "heartbeat interval")
		snapAt     = flag.Int("snapshot-threshold", 1000, "entries between snapshots (0 disables)")
		logLevel   = flag.String("log-level", "info", "debug|info|warn|error")
	)
	flag.Parse()

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	peers, ids, err := parsePeers(*peersFlag)
	if err != nil {
		return err
	}
	clientURLs := peers
	if *clientFlag != "" {
		if clientURLs, _, err = parsePeers(*clientFlag); err != nil {
			return fmt.Errorf("-client-urls: %w", err)
		}
	}
	store, err := raft.NewFileStorage(*dataDir)
	if err != nil {
		return err
	}
	fsm := kv.NewFSM()
	node, err := raft.NewNode(raft.Config{
		ID: *id, Peers: ids,
		ElectionTimeout: *election, HeartbeatInterval: *heartbeat,
		SnapshotThreshold: *snapAt, Logger: logger,
	}, httptransport.New(peers), store, fsm)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	httptransport.Register(mux, node)
	kv.NewServer(&kv.Store{Node: node, FSM: fsm}, clientURLs).Register(mux)
	// Linearizable reads and writes wait for a commit; kv.Server bounds that
	// with its own per-request context, so these are outer safety limits.
	// ReadTimeout leaves room for a large InstallSnapshot body.
	srv := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	logger.Info("raftkv started", "version", version, "id", *id, "listen", *listen, "peers", ids)

	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
	}
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	node.Stop()
	return nil
}

// healthcheck asks the local node's /healthz over loopback.
func healthcheck(listen string) error {
	_, port, err := net.SplitHostPort(listen)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
