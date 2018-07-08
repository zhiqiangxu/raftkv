package server

// Config contains config for Server
type Config struct {
	// APIAddr is for public
	APIAddr string
	// RaftAddr is internal
	RaftAddr string
	// RaftDir stores raft data
	RaftDir string
	// Join specifies a node
	Join string
}
