package server

import (
	"encoding/binary"
	"errors"
)

// Server semantecs
type Server interface {
	ListenAndServe() error
	Set(key []byte, value []byte) error
	Delete(key []byte) error
	Get(key []byte) ([]byte, error)
	Dump() map[string]string
	Join(raftAddr, nodeID string) error
}

// Cmd ops server support
type Cmd uint32

const (
	// SetCmd do set
	SetCmd Cmd = iota
	// SetRespCmd is resp for SetCmd
	SetRespCmd
	// GetCmd do get
	GetCmd
	// GetRespCmd is resp for GetCmd
	GetRespCmd
	// DeleteCmd do delete
	DeleteCmd
	// DeleteRespCmd is resp for DeleteCmd
	DeleteRespCmd
	// DumpCmd do get all
	DumpCmd
	// DumpRespCmd is resp for DumpCmd
	DumpRespCmd
	// JoinCmd joins a new node
	JoinCmd
	// JoinRespCmd is resp for JoinCmd
	JoinRespCmd
	// NoCmd is like 404 for http
	NoCmd
	// ErrCmd is when error happens
	ErrCmd
)

var (
	// ErrCmdNotFound is like 404
	ErrCmdNotFound = errors.New("cmd not found")
)

// SetParam is param for set
type SetParam struct {
	Key   []byte
	Value []byte
}

// GetParam is param for set
type GetParam struct {
	Key []byte
}

// DelParam is param for del
type DelParam struct {
	Key []byte
}

// JoinParam for join
type JoinParam struct {
	// RaftAddr for node
	RaftAddr string
}

// GetResp is resp for GetParam
type GetResp struct {
	Resp []byte
}

// DumpResp is resp for dump
type DumpResp struct {
	M map[string]string
}

// MakePacket generate packet
func MakePacket(requestID uint64, cmd Cmd, payload []byte) []byte {
	length := 12 + uint32(len(payload))
	buf := make([]byte, 4+length)
	binary.BigEndian.PutUint32(buf, length)
	binary.BigEndian.PutUint64(buf[4:], requestID)
	binary.BigEndian.PutUint32(buf[12:], uint32(cmd))
	copy(buf[16:], payload)
	return buf
}
