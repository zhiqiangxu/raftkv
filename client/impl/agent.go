package impl

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"qpush/modules/logger"
	"raftkv/client"
	"raftkv/modules/gob"
	"raftkv/modules/stream"
	"raftkv/server"
)

var (
	// ErrJoinNodeFail fail to join node
	ErrJoinNodeFail = errors.New("fail to join node")
)

// Agent is model for agent
type Agent struct {
}

// Dial creates a connection to server
func (a *Agent) Dial(address string) (client.Connection, error) {

	conn, err := net.Dial("tcp", address)
	if err != nil {

		return nil, err
	}

	return &Connection{conn: conn}, nil
}

// Connection the data struct for underlying connection
type Connection struct {
	conn net.Conn
}

// Set do set
func (c *Connection) Set(key []byte, value []byte) error {

	p := server.SetParam{Key: key, Value: value}

	bytes, err := gob.ToBytes(&p)
	if err != nil {
		return err
	}

	requestID := PoorManUUID()
	var errResp error
	err = c.sendCmdBlocking(requestID, server.SetCmd, bytes, func(cmd server.Cmd, bytes []byte) {

		if cmd != server.SetRespCmd {
			errResp = errors.New(string(bytes))
		}
	})

	if err != nil {
		return err
	}

	return errResp
}

// PoorManUUID generate a uint64 uuid
func PoorManUUID() (result uint64) {
	buf := make([]byte, 8)
	rand.Read(buf)
	result = binary.LittleEndian.Uint64(buf)
	if result == 0 {
		result = math.MaxUint64
	}
	return
}

// Delete do delete
func (c *Connection) Delete(key []byte) error {
	p := server.DelParam{Key: key}

	bytes, err := gob.ToBytes(&p)
	if err != nil {
		return err
	}

	requestID := PoorManUUID()

	return c.sendCmdBlocking(requestID, server.DeleteCmd, bytes, func(cmd server.Cmd, bytes []byte) {

	})
}

func (c *Connection) sendCmdBlocking(requestID uint64, cmd server.Cmd, bytes []byte, f func(server.Cmd, []byte)) error {

	packet := server.MakePacket(requestID, cmd, bytes)

	w := stream.NewWriter(c.conn)
	_, err := w.Write(packet)
	if err != nil {
		return err
	}

	return c.Subscribe(NewCallBack(func(ID uint64, cmd server.Cmd, bytes []byte) bool {
		logger.Info(requestID, cmd)
		if ID == requestID {
			f(cmd, bytes)
			return false
		}
		return true
	}))

}

// Get do get
func (c *Connection) Get(key []byte) ([]byte, error) {
	p := server.GetParam{Key: key}

	bytes, err := gob.ToBytes(&p)

	var resultBytes []byte
	requestID := PoorManUUID()
	err = c.sendCmdBlocking(requestID, server.GetCmd, bytes, func(cmd server.Cmd, bytes []byte) {
		resultBytes = bytes
	})
	if err != nil {
		return nil, err
	}
	return resultBytes, nil
}

// Dump do dump
func (c *Connection) Dump() (map[string][]byte, error) {

	return nil, nil
}

// Join to join
func (c *Connection) Join(raftAddr, nodeID string) error {

	p := server.JoinParam{RaftAddr: raftAddr, NodeID: nodeID}

	bytes, err := gob.ToBytes(&p)
	if err != nil {
		return err
	}

	suc := false
	requestID := PoorManUUID()
	err = c.sendCmdBlocking(requestID, server.JoinCmd, bytes, func(cmd server.Cmd, bytes []byte) {
		if cmd == server.JoinRespCmd {
			suc = true
		}
	})
	if suc {
		return nil
	}

	return ErrJoinNodeFail

}

// Subscribe start subscribe mode
func (c *Connection) Subscribe(cb client.OnResponse) error {

	r := stream.NewReader(c.conn)
	for {
		logger.Info("Subscribe1")
		size, err := r.ReadUint32()
		if err != nil {
			return err
		}
		logger.Info("Subscribe2")
		payload := make([]byte, size)
		err = r.ReadBytes(payload)
		if err != nil {
			return err
		}
		requestID := binary.BigEndian.Uint64(payload)
		cmd := server.Cmd(binary.BigEndian.Uint32(payload[8:]))
		ok := cb.Call(requestID, cmd, payload[12:])
		if !ok {
			return nil
		}
	}
}

// Close close the connection
func (c *Connection) Close() error {
	return c.conn.Close()
}
