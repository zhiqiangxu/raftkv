package impl

import "raftkv/server"

// OnResponse is data struct for callback
type OnResponse struct {
	f func(uint64, server.Cmd, []byte) bool
}

// NewCallBack creates an instance
func NewCallBack(cb func(uint64, server.Cmd, []byte) bool) *OnResponse {
	return &OnResponse{f: cb}
}

// Call is called when message arived
func (cb *OnResponse) Call(requestID uint64, cmd server.Cmd, bytes []byte) bool {
	return cb.f(requestID, cmd, bytes)
}
