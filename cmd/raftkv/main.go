// Command raftkv runs one node of a replicated key-value store.
//
//	raftkv -id n1 -listen :7001 -data /var/lib/raftkv \
//	  -peers n1=http://n1:7001,n2=http://n2:7001,n3=http://n3:7001
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
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
		id        = flag.String("id", os.Getenv("RAFTKV_ID"), "node id (must appear in -peers)")
		listen    = flag.String("listen", envOr("RAFTKV_LISTEN", ":7001"), "listen address for client API and raft RPCs")
		peersFlag = flag.String("peers", os.Getenv("RAFTKV_PEERS"), "comma-separated id=url list of all members")
		dataDir   = flag.String("data", envOr("RAFTKV_DATA", "./data"), "data directory")
		election  = flag.Duration("election-timeout", 500*time.Millisecond, "minimum election timeout")
		heartbeat = flag.Duration("heartbeat", 100*time.Millisecond, "heartbeat interval")
		snapAt    = flag.Int("snapshot-threshold", 1000, "entries between snapshots (0 disables)")
		logLevel  = flag.String("log-level", "info", "debug|info|warn|error")
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
	kv.NewServer(&kv.Store{Node: node, FSM: fsm}, peers).Register(mux)
	srv := &http.Server{Addr: *listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

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

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
