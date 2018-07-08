package client

import "raftkv/server"

// Agent is only different from client when dial
type Agent interface {
	Dial(address string) (Connection, error)
}

// Connection is a connection to server
type Connection interface {
	Set(key []byte, value []byte) error
	Delete(key []byte) error
	Get(key []byte) ([]byte, error)
	Dump() (map[string][]byte, error)
	Join(raftAddr, nodeID string) error
	Subscribe(cb OnResponse) error
	Close() error
}

// OnResponse is interface for client callback
type OnResponse interface {
	Call(uint64, server.Cmd, []byte) bool
}
