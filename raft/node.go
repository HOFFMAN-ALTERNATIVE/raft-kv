package raft

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

// Transport delivers RPCs to peers. Implementations must be safe for
// concurrent use and should honour ctx deadlines.
type Transport interface {
	RequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(ctx context.Context, peer string, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Config configures a Node.
type Config struct {
	// ID of this node; must appear in Peers.
	ID string
	// Peers lists every voting member, including ID. Membership is static.
	Peers []string
	// ElectionTimeout is the minimum election timeout; the effective timeout
	// is randomised in [ElectionTimeout, 2*ElectionTimeout).
	ElectionTimeout time.Duration
	// HeartbeatInterval must be well below ElectionTimeout.
	HeartbeatInterval time.Duration
	// SnapshotThreshold triggers log compaction once this many applied
	// entries sit in the log above the last snapshot. 0 disables snapshots.
	SnapshotThreshold int
	// MaxEntriesPerAppend caps a single AppendEntries batch.
	MaxEntriesPerAppend int
	// Logger receives structured events; nil discards them.
	Logger *slog.Logger
}

func (c *Config) setDefaults() error {
	if c.ID == "" {
		return fmt.Errorf("raft: empty node id")
	}
	if !slices.Contains(c.Peers, c.ID) {
		return fmt.Errorf("raft: node %q not in peers %v", c.ID, c.Peers)
	}
	if c.ElectionTimeout == 0 {
		c.ElectionTimeout = 300 * time.Millisecond
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = c.ElectionTimeout / 6
	}
	if c.HeartbeatInterval >= c.ElectionTimeout {
		return fmt.Errorf("raft: heartbeat %v must be < election timeout %v", c.HeartbeatInterval, c.ElectionTimeout)
	}
	if c.MaxEntriesPerAppend <= 0 {
		c.MaxEntriesPerAppend = 256
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return nil
}

type waiter struct {
	term uint64
	ch   chan applyResult
}

type applyResult struct {
	val any
	err error
}

// Node is one Raft replica. All exported methods are safe for concurrent use.
type Node struct {
	cfg   Config
	trans Transport
	store Storage
	fsm   StateMachine
	log   *slog.Logger
	peers []string // everyone except self

	mu sync.Mutex
	// cond is broadcast on any change that blocked callers may wait for:
	// commit/apply progress, leadership acks, role changes, stop.
	cond      *sync.Cond
	applyCond *sync.Cond

	// persistent state (§5.2, Figure 2)
	currentTerm uint64
	votedFor    string
	entries     []Entry // entries[0] is the snapshot sentinel

	// volatile state
	role              Role
	leaderID          string
	commitIndex       uint64
	lastApplied       uint64
	electionDeadline  time.Time
	lastLeaderContact time.Time
	snapshot          []byte // latest snapshot payload, for InstallSnapshot
	pendingSnapshot   *snapshotEnvelope

	// leader state
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	lastAck    map[string]time.Time
	// ReadIndex bookkeeping: every outgoing AppendEntries carries the
	// current hbSeq; a same-term reply proves the peer still accepted us as
	// leader at or after that point.
	hbSeq   uint64
	ackSeq  map[string]uint64
	waiters map[uint64]waiter

	replicate map[string]chan struct{}
	stopped   bool
	stopCh    chan struct{}
	wg        sync.WaitGroup

	stats struct{ snapshotsTaken, snapshotsInstalled, elections uint64 }
}

// NewNode restores state from store and starts the node's goroutines.
func NewNode(cfg Config, trans Transport, store Storage, fsm StateMachine) (*Node, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	n := &Node{
		cfg:       cfg,
		trans:     trans,
		store:     store,
		fsm:       fsm,
		log:       cfg.Logger.With("node", cfg.ID),
		entries:   []Entry{{}},
		waiters:   map[uint64]waiter{},
		replicate: map[string]chan struct{}{},
		stopCh:    make(chan struct{}),
	}
	n.cond = sync.NewCond(&n.mu)
	n.applyCond = sync.NewCond(&n.mu)
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			n.peers = append(n.peers, p)
			n.replicate[p] = make(chan struct{}, 1)
		}
	}
	if err := n.restore(); err != nil {
		return nil, err
	}
	n.resetElectionTimer()

	n.wg.Add(2 + len(n.peers))
	go n.ticker()
	go n.applier()
	for _, p := range n.peers {
		go n.replicator(p)
	}
	return n, nil
}

func (n *Node) restore() error {
	snapBytes, err := n.store.LoadSnapshot()
	if err != nil {
		return fmt.Errorf("raft: load snapshot: %w", err)
	}
	var snap snapshotEnvelope
	if len(snapBytes) > 0 {
		if err := decodeGob(snapBytes, &snap); err != nil {
			return fmt.Errorf("raft: decode snapshot: %w", err)
		}
	}
	stateBytes, err := n.store.LoadState()
	if err != nil {
		return fmt.Errorf("raft: load state: %w", err)
	}
	if len(stateBytes) > 0 {
		var ps persistentState
		if err := decodeGob(stateBytes, &ps); err != nil {
			return fmt.Errorf("raft: decode state: %w", err)
		}
		n.currentTerm, n.votedFor, n.entries = ps.CurrentTerm, ps.VotedFor, ps.Log
		if len(n.entries) == 0 {
			n.entries = []Entry{{}}
		}
	}
	if snap.LastIncludedIndex > n.base() {
		// Crashed after SaveSnapshot but before the compacting SaveState.
		n.compactTo(snap.LastIncludedIndex, snap.LastIncludedTerm)
	}
	if len(snapBytes) > 0 {
		if err := n.fsm.Restore(snap.Data); err != nil {
			return fmt.Errorf("raft: restore state machine: %w", err)
		}
		n.snapshot = snap.Data
	}
	n.commitIndex = n.base()
	n.lastApplied = n.base()
	return nil
}

// ---- log helpers (n.mu held) -------------------------------------------

func (n *Node) base() uint64      { return n.entries[0].Index }
func (n *Node) lastIndex() uint64 { return n.entries[len(n.entries)-1].Index }
func (n *Node) lastTerm() uint64  { return n.entries[len(n.entries)-1].Term }
func (n *Node) entry(i uint64) *Entry {
	return &n.entries[i-n.base()]
}
func (n *Node) termAt(i uint64) uint64 { return n.entry(i).Term }

// compactTo discards everything up to and including index, keeping any
// suffix that agrees with (index, term).
func (n *Node) compactTo(index, term uint64) {
	if index <= n.lastIndex() && index >= n.base() && n.termAt(index) == term {
		n.entries = slices.Clone(n.entries[index-n.base():])
	} else {
		n.entries = []Entry{{}}
	}
	n.entries[0] = Entry{Index: index, Term: term}
}

func (n *Node) majority() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) persist() {
	err := n.store.SaveState(encodeGob(persistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.entries,
	}))
	if err != nil {
		// Raft assumes stable storage; continuing after a failed write could
		// violate safety (e.g. double voting), so fail-stop.
		panic(fmt.Sprintf("raft: persist state: %v", err))
	}
}

func (n *Node) persistWithSnapshot(snap []byte) {
	err := n.store.SaveSnapshot(encodeGob(snapshotEnvelope{
		LastIncludedIndex: n.base(),
		LastIncludedTerm:  n.entries[0].Term,
		Data:              snap,
	}))
	if err != nil {
		panic(fmt.Sprintf("raft: persist snapshot: %v", err))
	}
	n.persist()
}

func (n *Node) resetElectionTimer() {
	d := n.cfg.ElectionTimeout + rand.N(n.cfg.ElectionTimeout) //nolint:gosec // jitter, not crypto
	n.electionDeadline = time.Now().Add(d)
}

// ---- role transitions (n.mu held) --------------------------------------

func (n *Node) becomeFollower(term uint64) {
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
		n.leaderID = ""
		n.persist()
	}
	if n.role == Leader {
		n.log.Info("stepping down", "term", n.currentTerm)
		n.failWaiters(ErrLeadershipLost)
	}
	n.role = Follower
	n.cond.Broadcast()
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.cfg.ID
	n.nextIndex = map[string]uint64{}
	n.matchIndex = map[string]uint64{}
	n.lastAck = map[string]time.Time{}
	n.ackSeq = map[string]uint64{}
	now := time.Now()
	for _, p := range n.peers {
		n.nextIndex[p] = n.lastIndex() + 1
		n.lastAck[p] = now
	}
	n.entries = append(n.entries, Entry{Index: n.lastIndex() + 1, Term: n.currentTerm, Type: EntryNoop})
	n.persist()
	n.log.Info("became leader", "term", n.currentTerm, "last_index", n.lastIndex())
	n.advanceCommit()
	n.triggerAll()
	n.cond.Broadcast()
}

func (n *Node) failWaiters(err error) {
	for idx, w := range n.waiters {
		w.ch <- applyResult{err: err}
		delete(n.waiters, idx)
	}
}

func (n *Node) triggerAll() {
	for _, ch := range n.replicate {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// ---- elections ---------------------------------------------------------

func (n *Node) ticker() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.HeartbeatInterval / 2)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
		}
		n.mu.Lock()
		switch {
		case n.role == Leader:
			n.checkQuorum()
		case time.Now().After(n.electionDeadline):
			n.startElection(true)
		}
		n.mu.Unlock()
	}
}

// checkQuorum makes a leader that cannot reach a majority step down, so a
// partitioned leader stops accepting writes it can never commit (§6.2).
func (n *Node) checkQuorum() {
	active := 1
	now := time.Now()
	for _, p := range n.peers {
		if now.Sub(n.lastAck[p]) < n.cfg.ElectionTimeout {
			active++
		}
	}
	if active < n.majority() {
		n.log.Warn("check-quorum failed, stepping down", "term", n.currentTerm, "active", active)
		n.becomeFollower(n.currentTerm)
		n.leaderID = ""
		n.resetElectionTimer()
	}
}

func (n *Node) startElection(pre bool) {
	n.resetElectionTimer()
	term := n.currentTerm + 1
	if pre {
		n.role = PreCandidate
	} else {
		n.currentTerm = term
		n.votedFor = n.cfg.ID
		n.role = Candidate
		n.leaderID = ""
		n.stats.elections++
		n.persist()
		n.log.Info("starting election", "term", term)
	}
	startTerm := n.currentTerm
	args := &RequestVoteArgs{
		Term: term, CandidateID: n.cfg.ID,
		LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm(), PreVote: pre,
	}
	votes := 1
	won := func() {
		if pre {
			n.startElection(false)
		} else {
			n.becomeLeader()
		}
	}
	if votes >= n.majority() {
		won()
		return
	}
	wantRole := Candidate
	if pre {
		wantRole = PreCandidate
	}
	for _, p := range n.peers {
		go func(peer string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeout)
			defer cancel()
			reply, err := n.trans.RequestVote(ctx, peer, args)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.stopped {
				return
			}
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				n.resetElectionTimer()
				return
			}
			if n.role != wantRole || n.currentTerm != startTerm || !reply.VoteGranted {
				return
			}
			votes++
			if votes == n.majority() {
				won()
			}
		}(p)
	}
}

// HandleRequestVote is the RequestVote RPC handler.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) (*RequestVoteReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	upToDate := args.LastLogTerm > n.lastTerm() ||
		(args.LastLogTerm == n.lastTerm() && args.LastLogIndex >= n.lastIndex())

	if args.PreVote {
		// Leader stickiness: refuse while we believe a live leader exists,
		// so a node returning from a partition cannot depose it.
		heardRecently := n.role == Leader ||
			(n.leaderID != "" && time.Since(n.lastLeaderContact) < n.cfg.ElectionTimeout)
		granted := args.Term > n.currentTerm && upToDate && !heardRecently
		return &RequestVoteReply{Term: n.currentTerm, VoteGranted: granted}, nil
	}

	if args.Term < n.currentTerm {
		return &RequestVoteReply{Term: n.currentTerm}, nil
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}
	reply := &RequestVoteReply{Term: n.currentTerm}
	if (n.votedFor == "" || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		n.persist()
		n.resetElectionTimer()
		reply.VoteGranted = true
	}
	return reply, nil
}

// ---- replication -------------------------------------------------------

func (n *Node) replicator(peer string) {
	defer n.wg.Done()
	hb := time.NewTicker(n.cfg.HeartbeatInterval)
	defer hb.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-hb.C:
		case <-n.replicate[peer]:
		}
		for n.replicateOnce(peer) {
			select {
			case <-n.stopCh:
				return
			default:
			}
		}
	}
}

// replicateOnce sends one RPC to peer and reports whether more data is
// waiting to be sent immediately.
func (n *Node) replicateOnce(peer string) bool {
	n.mu.Lock()
	if n.role != Leader || n.stopped {
		n.mu.Unlock()
		return false
	}
	term, seq := n.currentTerm, n.hbSeq
	next := n.nextIndex[peer]
	if next <= n.base() {
		args := &InstallSnapshotArgs{
			Term: term, LeaderID: n.cfg.ID,
			LastIncludedIndex: n.base(), LastIncludedTerm: n.entries[0].Term,
			Data: n.snapshot,
		}
		n.mu.Unlock()
		reply, err := n.call(func(ctx context.Context) (uint64, error) {
			r, err := n.trans.InstallSnapshot(ctx, peer, args)
			if err != nil {
				return 0, err
			}
			return r.Term, nil
		})
		if err != nil {
			return false
		}
		n.mu.Lock()
		defer n.mu.Unlock()
		if !n.acceptReply(peer, term, seq, reply) {
			return false
		}
		n.matchIndex[peer] = max(n.matchIndex[peer], args.LastIncludedIndex)
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommit()
		return n.nextIndex[peer] <= n.lastIndex()
	}

	prev := next - 1
	end := min(n.lastIndex(), prev+uint64(n.cfg.MaxEntriesPerAppend)) //nolint:gosec // validated > 0 in setDefaults
	args := &AppendEntriesArgs{
		Term: term, LeaderID: n.cfg.ID,
		PrevLogIndex: prev, PrevLogTerm: n.termAt(prev),
		LeaderCommit: n.commitIndex,
	}
	if end > prev {
		args.Entries = slices.Clone(n.entries[next-n.base() : end-n.base()+1])
	}
	n.mu.Unlock()

	var reply *AppendEntriesReply
	replyTerm, err := n.call(func(ctx context.Context) (uint64, error) {
		r, err := n.trans.AppendEntries(ctx, peer, args)
		if err != nil {
			return 0, err
		}
		reply = r
		return r.Term, nil
	})
	if err != nil {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.acceptReply(peer, term, seq, replyTerm) {
		return false
	}
	if reply.Success {
		match := args.PrevLogIndex + uint64(len(args.Entries))
		n.matchIndex[peer] = max(n.matchIndex[peer], match)
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommit()
		return n.nextIndex[peer] <= n.lastIndex()
	}
	// Fast log backtracking (§5.3 optimisation).
	next = reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		for i := n.lastIndex(); i > n.base(); i-- {
			if t := n.termAt(i); t == reply.ConflictTerm {
				next = i + 1
				break
			} else if t < reply.ConflictTerm {
				break
			}
		}
	}
	n.nextIndex[peer] = max(1, min(next, n.lastIndex()+1), n.matchIndex[peer]+1)
	return true
}

func (n *Node) call(fn func(ctx context.Context) (uint64, error)) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeout)
	defer cancel()
	return fn(ctx)
}

// acceptReply handles the common part of every leader-side reply and reports
// whether the reply is still relevant.
func (n *Node) acceptReply(peer string, term, seq, replyTerm uint64) bool {
	if replyTerm > n.currentTerm {
		n.becomeFollower(replyTerm)
		n.resetElectionTimer()
		return false
	}
	if n.role != Leader || n.currentTerm != term {
		return false
	}
	n.lastAck[peer] = time.Now()
	if seq > n.ackSeq[peer] {
		n.ackSeq[peer] = seq
		n.cond.Broadcast()
	}
	return true
}

// advanceCommit commits the highest index replicated on a majority whose
// entry is from the current term (§5.4.2: never commit old-term entries by
// counting replicas).
func (n *Node) advanceCommit() {
	for idx := n.lastIndex(); idx > n.commitIndex && idx > n.base(); idx-- {
		if n.termAt(idx) != n.currentTerm {
			break
		}
		count := 1
		for _, p := range n.peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = idx
			n.applyCond.Signal()
			n.cond.Broadcast()
			n.triggerAll() // propagate the new commit index promptly
			return
		}
	}
}

// HandleAppendEntries is the AppendEntries RPC handler.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	if args.Term < n.currentTerm {
		return &AppendEntriesReply{Term: n.currentTerm}, nil
	}
	if args.Term > n.currentTerm || n.role != Follower {
		n.becomeFollower(args.Term)
	}
	n.leaderID = args.LeaderID
	n.lastLeaderContact = time.Now()
	n.resetElectionTimer()
	reply := &AppendEntriesReply{Term: n.currentTerm}

	switch {
	case args.PrevLogIndex < n.base():
		// Prefix already compacted into our snapshot: ask for the entry
		// after it.
		reply.ConflictIndex = n.base() + 1
		return reply, nil
	case args.PrevLogIndex > n.lastIndex():
		reply.ConflictIndex = n.lastIndex() + 1
		return reply, nil
	case n.termAt(args.PrevLogIndex) != args.PrevLogTerm:
		ct := n.termAt(args.PrevLogIndex)
		first := args.PrevLogIndex
		for first-1 > n.base() && n.termAt(first-1) == ct {
			first--
		}
		reply.ConflictTerm, reply.ConflictIndex = ct, first
		return reply, nil
	}

	changed := false
	for i, e := range args.Entries {
		if e.Index > n.lastIndex() {
			n.entries = append(n.entries, args.Entries[i:]...)
			changed = true
			break
		}
		if n.termAt(e.Index) != e.Term {
			if e.Index <= n.commitIndex {
				panic(fmt.Sprintf("raft: leader %s tried to overwrite committed entry %d", args.LeaderID, e.Index))
			}
			n.entries = append(n.entries[:e.Index-n.base()], args.Entries[i:]...)
			changed = true
			break
		}
	}
	if changed {
		n.persist()
	}
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+uint64(len(args.Entries)))
		n.applyCond.Signal()
		n.cond.Broadcast()
	}
	reply.Success = true
	return reply, nil
}

// HandleInstallSnapshot is the InstallSnapshot RPC handler.
func (n *Node) HandleInstallSnapshot(args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	if args.Term < n.currentTerm {
		return &InstallSnapshotReply{Term: n.currentTerm}, nil
	}
	if args.Term > n.currentTerm || n.role != Follower {
		n.becomeFollower(args.Term)
	}
	n.leaderID = args.LeaderID
	n.lastLeaderContact = time.Now()
	n.resetElectionTimer()
	if args.LastIncludedIndex <= n.commitIndex {
		return &InstallSnapshotReply{Term: n.currentTerm}, nil // stale or duplicate
	}
	n.compactTo(args.LastIncludedIndex, args.LastIncludedTerm)
	n.snapshot = args.Data
	n.commitIndex = args.LastIncludedIndex
	n.persistWithSnapshot(args.Data)
	n.pendingSnapshot = &snapshotEnvelope{LastIncludedIndex: args.LastIncludedIndex, LastIncludedTerm: args.LastIncludedTerm, Data: args.Data}
	n.stats.snapshotsInstalled++
	n.log.Info("installed snapshot", "index", args.LastIncludedIndex, "term", args.LastIncludedTerm)
	n.applyCond.Signal()
	return &InstallSnapshotReply{Term: n.currentTerm}, nil
}

// ---- applying ----------------------------------------------------------

func (n *Node) applier() {
	defer n.wg.Done()
	n.mu.Lock()
	defer n.mu.Unlock()
	for {
		for !n.stopped && n.pendingSnapshot == nil && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			return
		}
		if snap := n.pendingSnapshot; snap != nil {
			n.pendingSnapshot = nil
			n.mu.Unlock()
			err := n.fsm.Restore(snap.Data)
			n.mu.Lock()
			if err != nil {
				panic(fmt.Sprintf("raft: restore snapshot: %v", err))
			}
			n.lastApplied = max(n.lastApplied, snap.LastIncludedIndex)
			n.cond.Broadcast()
			continue
		}
		batch := slices.Clone(n.entries[n.lastApplied+1-n.base() : n.commitIndex+1-n.base()])
		n.mu.Unlock()
		results := make([]any, len(batch))
		for i, e := range batch {
			if e.Type == EntryCommand {
				results[i] = n.fsm.Apply(e)
			}
		}
		n.mu.Lock()
		for i, e := range batch {
			n.lastApplied = max(n.lastApplied, e.Index)
			if w, ok := n.waiters[e.Index]; ok {
				delete(n.waiters, e.Index)
				if w.term == e.Term {
					w.ch <- applyResult{val: results[i]}
				} else {
					w.ch <- applyResult{err: ErrLeadershipLost}
				}
			}
		}
		n.cond.Broadcast()
		n.maybeSnapshot()
	}
}

// maybeSnapshot compacts the log once enough applied entries accumulate.
// Called by the applier with n.mu held; the FSM is only touched by the
// applier goroutine, so its state matches lastApplied while we unlock.
func (n *Node) maybeSnapshot() {
	if n.cfg.SnapshotThreshold <= 0 || n.pendingSnapshot != nil || n.lastApplied <= n.base() ||
		n.lastApplied-n.base() < uint64(n.cfg.SnapshotThreshold) { //nolint:gosec // checked > 0 above
		return
	}
	idx := n.lastApplied
	n.mu.Unlock()
	data, err := n.fsm.Snapshot()
	n.mu.Lock()
	if err != nil {
		n.log.Error("snapshot failed", "err", err)
		return
	}
	if idx <= n.base() {
		return // an installed snapshot overtook us
	}
	n.compactTo(idx, n.termAt(idx))
	n.snapshot = data
	n.persistWithSnapshot(data)
	n.stats.snapshotsTaken++
	n.log.Debug("took snapshot", "index", idx, "log_len", len(n.entries))
}

// ---- client API --------------------------------------------------------

// Propose appends cmd to the log and blocks until it is committed and
// applied, returning the state machine's result. It fails fast with
// *NotLeaderError on followers. ErrLeadershipLost and context errors mean
// the outcome is unknown: the entry may still commit, so clients that retry
// must make commands idempotent (see the kv package's session table).
func (n *Node) Propose(ctx context.Context, cmd []byte) (any, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, ErrStopped
	}
	if n.role != Leader {
		leader := n.leaderID
		n.mu.Unlock()
		return nil, &NotLeaderError{Leader: leader}
	}
	idx := n.lastIndex() + 1
	n.entries = append(n.entries, Entry{Index: idx, Term: n.currentTerm, Type: EntryCommand, Data: cmd})
	n.persist()
	ch := make(chan applyResult, 1)
	n.waiters[idx] = waiter{term: n.currentTerm, ch: ch}
	n.advanceCommit() // single-node clusters commit immediately
	n.triggerAll()
	n.mu.Unlock()

	select {
	case r := <-ch:
		return r.val, r.err
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.waiters, idx)
		n.mu.Unlock()
		return nil, ctx.Err()
	}
}

// ReadIndex implements linearizable reads without writing to the log
// (thesis §6.4): it records the commit index, confirms leadership with a
// round of heartbeats acknowledged by a majority, and waits until the state
// machine has applied up to that index. After it returns nil the caller may
// read local state and observe every write committed before the call.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	stop := context.AfterFunc(ctx, func() {
		n.mu.Lock()
		n.cond.Broadcast()
		n.mu.Unlock()
	})
	defer stop()

	if n.role != Leader {
		return 0, &NotLeaderError{Leader: n.leaderID}
	}
	term := n.currentTerm
	// A fresh leader does not know the commit index until its no-op commits.
	for n.role == Leader && n.currentTerm == term && n.termAt(n.commitIndex) != term {
		if err := n.waitErr(ctx); err != nil {
			return 0, err
		}
	}
	if n.role != Leader || n.currentTerm != term {
		return 0, ErrLeadershipLost
	}
	readIdx := n.commitIndex
	n.hbSeq++
	seq := n.hbSeq
	n.triggerAll()
	for {
		if n.role != Leader || n.currentTerm != term {
			return 0, ErrLeadershipLost
		}
		acks := 1
		for _, p := range n.peers {
			if n.ackSeq[p] >= seq {
				acks++
			}
		}
		if acks >= n.majority() {
			break
		}
		if err := n.waitErr(ctx); err != nil {
			return 0, err
		}
	}
	for n.lastApplied < readIdx {
		if err := n.waitErr(ctx); err != nil {
			return 0, err
		}
	}
	return readIdx, nil
}

func (n *Node) waitErr(ctx context.Context) error {
	if n.stopped {
		return ErrStopped
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	n.cond.Wait()
	if n.stopped {
		return ErrStopped
	}
	return ctx.Err()
}

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID: n.cfg.ID, Role: n.role.String(), Term: n.currentTerm, Leader: n.leaderID,
		CommitIndex: n.commitIndex, LastApplied: n.lastApplied,
		LastLogIndex: n.lastIndex(), SnapshotIndex: n.base(),
		SnapshotsTaken: n.stats.snapshotsTaken, SnapshotsInstalled: n.stats.snapshotsInstalled,
		Elections: n.stats.elections,
	}
}

// State returns the current term and whether this node believes it leads.
func (n *Node) State() (term uint64, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role == Leader
}

// Stop halts all goroutines. Persisted state remains in Storage, so a new
// Node built on the same Storage models a crash + restart.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	n.failWaiters(ErrStopped)
	n.applyCond.Broadcast()
	n.cond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
}
