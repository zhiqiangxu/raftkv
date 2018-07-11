package impl

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"raftkv/server"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

var (
	// ErrNotLeader when write on non-leader
	ErrNotLeader = errors.New("not leader")
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
	leaderWaitDelay     = 1000 * time.Millisecond
)

// Store is a simple key-value store, where all changes are made via Raft consensus.
type Store struct {
	RaftDir  string
	RaftBind string

	m    sync.Map // The key-value store for the system.
	meta sync.Map // the meta info for nodes

	raft *raft.Raft // The consensus mechanism
}

// Open opens the store. If enableSingle is set, and there are no existing peers,
// then this node becomes the first node, and therefore leader, of the cluster.
// localID should be the server identifier for this node.
func (s *Store) Open(enableSingle bool, localID string) error {
	// Setup Raft configuration.
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", s.RaftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(s.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create the snapshot store. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(s.RaftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}

	// Create the log store and stable store.
	var logStore raft.LogStore
	var stableStore raft.StableStore
	boltDB, err := raftboltdb.NewBoltStore(filepath.Join(s.RaftDir, "raft.db"))
	logStore = boltDB
	stableStore = boltDB

	// Instantiate the Raft systems.
	ra, err := raft.NewRaft(config, (*fsm)(s), logStore, stableStore, snapshots, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	s.raft = ra

	if enableSingle {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	}

	return nil
}

// WaitForLeader blocks until a leader is detected
func (s *Store) WaitForLeader(done chan bool) string {
	tck := time.NewTicker(leaderWaitDelay)
	defer tck.Stop()

	for {
		select {
		case <-tck.C:
			l := string(s.raft.Leader())
			if l != "" {
				return l
			}
		case <-done:
			return ""
		}
	}
}

// Get returns the value for the given key.
func (s *Store) Get(key string) (data string, ok bool) {
	value, ok := s.m.Load(key)
	if ok {
		data = value.(string)
	}

	return
}

// Command is for kv op
type Command struct {
	Cmd   server.Cmd
	Key   string
	Value string
}

// Set implements set
func (s *Store) Set(key, value string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	c := &Command{
		Cmd:   server.SetCmd,
		Key:   key,
		Value: value,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return convertErr(f.Error())
}

// Delete deletes the given key.
func (s *Store) Delete(key string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	c := &Command{
		Cmd: server.DeleteCmd,
		Key: key,
	}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return convertErr(f.Error())
}

// Dump returns all data
func (s *Store) Dump() map[string]string {
	m := make(map[string]string)
	s.m.Range(func(key, value interface{}) bool {
		m[key.(string)] = value.(string)
		return true
	})
	return m
}

// Join joins a node, identified by nodeID and located at addr, to this store.
// The node must be ready to respond to Raft communications at that address.
func (s *Store) Join(nodeID, addr, apiAddr string) error {

	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}

	configFuture := s.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return convertErr(err)
	}

	for _, srv := range configFuture.Configuration().Servers {
		// If a node already exists with either the joining node's ID or address,
		// that node may need to be removed from the config first.
		if srv.ID == raft.ServerID(nodeID) || srv.Address == raft.ServerAddress(addr) {
			// However if *both* the ID and the address are the same, then nothing -- not even
			// a join operation -- is needed.
			if srv.Address == raft.ServerAddress(addr) && srv.ID == raft.ServerID(nodeID) {
				return nil
			}

			future := s.raft.RemoveServer(srv.ID, 0, 0)
			if err := future.Error(); err != nil {
				return convertErr(future.Error())
			}
		}
	}

	f := s.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if f.Error() != nil {
		return convertErr(f.Error())
	}

	return s.SetAPIAddr(nodeID, apiAddr)
}

func convertErr(err error) error {
	if err == raft.ErrNotLeader {
		return ErrNotLeader
	}

	return err
}

// LeaderAPIAddr returns leader api address
func (s *Store) LeaderAPIAddr() string {
	addr, ok := s.meta.Load(string(s.raft.Leader()))
	if !ok {
		return ""
	}
	return addr.(string)
}

// SetAPIAddr set api address for node
func (s *Store) SetAPIAddr(nodeID, apiAddr string) error {

	addr, ok := s.meta.Load(nodeID)
	if ok && addr.(string) == apiAddr {
		return nil
	}

	c := &Command{
		Cmd:   server.JoinCmd,
		Key:   nodeID,
		Value: apiAddr}
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}

	f := s.raft.Apply(b, raftTimeout)
	return convertErr(f.Error())
}

type fsm Store

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Cmd {
	case server.SetCmd:
		f.m.Store(c.Key, c.Value)
		return nil
	case server.DeleteCmd:
		f.m.Delete(c.Key)
		return nil
	case server.JoinCmd:
		fmt.Println("JoinCmd called", c.Key, c.Value)
		f.meta.Store(c.Key, c.Value)
		value, ok := f.meta.Load(c.Key)
		fmt.Println("LeaderAPIAddr", (*Store)(f).LeaderAPIAddr(), value.(string), ok)
		return nil
	default:
		panic(fmt.Sprintf("unrecognized command: %d", c.Cmd))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	o := make(map[string]string)
	f.m.Range(func(key, value interface{}) bool {
		o[key.(string)] = value.(string)
		return true
	})
	meta := make(map[string]interface{})
	f.meta.Range(func(key, value interface{}) bool {
		meta[key.(string)] = value
		return true
	})
	return &fsmSnapshot{store: o, meta: meta}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {

	o := make(map[string]interface{})
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}
	ostore, _ := o["store"].(map[string]string)
	ometa, _ := o["meta"].(map[string]interface{})

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	m := &f.m
	meta := &f.meta
	m.Range(func(key, value interface{}) bool {
		m.Delete(key)
		return true
	})
	meta.Range(func(key, value interface{}) bool {
		m.Delete(key)
		return true
	})
	for k, v := range ostore {
		m.Store(k, v)
	}

	for k, v := range ometa {
		meta.Store(k, v)
	}
	return nil
}

type fsmSnapshot struct {
	store map[string]string
	meta  map[string]interface{}
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		// Encode data.
		data := map[string]interface{}{"store": f.store, "meta": f.meta}
		b, err := json.Marshal(data)
		if err != nil {
			return err
		}

		// Write data to sink.
		if _, err := sink.Write(b); err != nil {
			return err
		}

		// Close the sink.
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}

	return err
}

func (f *fsmSnapshot) Release() {}
