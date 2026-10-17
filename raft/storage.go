package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Storage persists Raft's durable state. SaveState must be atomic and durable
// before it returns (Raft's safety argument assumes votes and log entries
// survive crashes). SaveSnapshot is always called *before* the SaveState that
// compacts the log onto it, so a crash between the two leaves a snapshot that
// is newer than the log base — which recovery handles — never the reverse.
type Storage interface {
	SaveState(state []byte) error
	SaveSnapshot(snapshot []byte) error
	LoadState() ([]byte, error)
	LoadSnapshot() ([]byte, error)
}

// MemoryStorage keeps state in memory. It survives Node restarts within a
// process, which is what the crash/recovery tests need.
type MemoryStorage struct {
	mu       sync.Mutex
	state    []byte
	snapshot []byte
}

// NewMemoryStorage returns an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

// SaveState implements Storage.
func (m *MemoryStorage) SaveState(s []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = bytes.Clone(s)
	return nil
}

// SaveSnapshot implements Storage.
func (m *MemoryStorage) SaveSnapshot(s []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshot = bytes.Clone(s)
	return nil
}

// LoadState implements Storage.
func (m *MemoryStorage) LoadState() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return bytes.Clone(m.state), nil
}

// LoadSnapshot implements Storage.
func (m *MemoryStorage) LoadSnapshot() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return bytes.Clone(m.snapshot), nil
}

// StateSize returns the size of the persisted state in bytes (used by tests to
// prove that compaction bounds the log).
func (m *MemoryStorage) StateSize() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.state)
}

// FileStorage persists state into a directory using write-to-temp, fsync,
// rename, fsync(dir) — the standard crash-safe replace sequence.
type FileStorage struct {
	dir string
}

// NewFileStorage creates dir if needed.
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	return &FileStorage{dir: dir}, nil
}

// SaveState implements Storage.
func (f *FileStorage) SaveState(s []byte) error { return f.write("state.bin", s) }

// SaveSnapshot implements Storage.
func (f *FileStorage) SaveSnapshot(s []byte) error { return f.write("snapshot.bin", s) }

// LoadState implements Storage.
func (f *FileStorage) LoadState() ([]byte, error) { return f.read("state.bin") }

// LoadSnapshot implements Storage.
func (f *FileStorage) LoadSnapshot() ([]byte, error) { return f.read("snapshot.bin") }

func (f *FileStorage) read(name string) ([]byte, error) {
	b, err := os.ReadFile(filepath.Join(f.dir, name))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return b, err
}

func (f *FileStorage) write(name string, data []byte) error {
	tmp, err := os.CreateTemp(f.dir, name+".tmp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name()) //nolint:errcheck // best effort; gone after rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close() //nolint:errcheck,gosec // already failing
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close() //nolint:errcheck,gosec // already failing
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), filepath.Join(f.dir, name)); err != nil {
		return err
	}
	d, err := os.Open(f.dir)
	if err != nil {
		return err
	}
	defer d.Close() //nolint:errcheck // read-only handle
	return d.Sync()
}

// persistentState is what SaveState encodes. Log[0] is a sentinel holding the
// index/term of the last entry covered by the snapshot.
type persistentState struct {
	CurrentTerm uint64
	VotedFor    string
	Log         []Entry
}

type snapshotEnvelope struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

func encodeGob(v any) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		panic(fmt.Sprintf("raft: gob encode: %v", err)) // only on programmer error
	}
	return buf.Bytes()
}

func decodeGob(b []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(b)).Decode(v)
}
