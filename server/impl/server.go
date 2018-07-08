package impl

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"os/signal"
	"qpush/modules/logger"
	cimpl "raftkv/client/impl"
	"raftkv/modules/gob"
	"raftkv/modules/stream"
	"raftkv/server"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
)

const (
	// DefaultAcceptTimeout is as named
	DefaultAcceptTimeout = 5 * time.Second
	// DefaultWriteTimeout is default timeout for write in seconds
	DefaultWriteTimeout = 10
)

// NewServer create a server
func NewServer(config server.Config) *Server {
	return &Server{config: config, done: make(chan bool)}
}

// Server model
type Server struct {
	store         *Store
	config        server.Config
	upTime        time.Time
	routinesGroup sync.WaitGroup
	done          chan bool
}

func (s *Server) goFunc(f func()) {
	s.routinesGroup.Add(1)
	go func() {
		defer s.routinesGroup.Done()
		f()
	}()
}

// ListenAndServe starts server
func (s *Server) ListenAndServe() error {

	s.store = &Store{RaftBind: s.config.RaftAddr, RaftDir: s.config.RaftDir}
	err := s.store.Open(s.config.Join == "", s.config.RaftAddr)
	if err != nil {
		return err
	}

	if s.config.Join != "" {

		agent := cimpl.Agent{}
		conn, err := agent.Dial(s.config.Join)
		if err != nil {
			return err
		}
		err = conn.Join(s.config.RaftAddr)
		if err != nil {
			return err
		}
		conn.Close()

	}

	quitChan := make(chan os.Signal, 1)
	signal.Notify(quitChan, os.Interrupt, os.Kill, syscall.SIGTERM)

	s.goFunc(func() {
		s.handleSignal(quitChan)
	})
	s.goFunc(s.listenAndServe)

	s.waitShutdown()

	return nil
}

func (s *Server) handleSignal(quitChan chan os.Signal) {
	<-quitChan
	close(s.done)
}

func (s *Server) waitShutdown() {
	s.routinesGroup.Wait()
}

func (s *Server) listenAndServe() {

	listener, err := net.Listen("tcp", s.config.APIAddr)

	if err != nil {
		panic(fmt.Sprintf("listen failed: %s", s.config.APIAddr))
	}

	defer listener.Close()

	tcpListener, ok := listener.(*net.TCPListener)
	if !ok {
		panic("shouldn't happen")
	}

	for {
		select {
		case <-s.done:
			return
		default:
		}
		tcpListener.SetDeadline(time.Now().Add(DefaultAcceptTimeout))
		conn, err := listener.Accept()
		if err != nil {
			if opError, ok := err.(*net.OpError); ok && opError.Timeout() {
				// don't log the scheduled timeout
				continue
			}
			logger.Error(fmt.Sprintf("accept failed:%s", err))
		}

		s.goFunc(func() {
			s.handleConnection(conn)
		})
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error("recovered from panic in handleConnection", err, string(debug.Stack()))
		}
	}()

	writeChan := make(chan []byte, 30)
	closeChan := make(chan bool)
	defer s.closeConnection(conn, closeChan)

	s.goFunc(func() {
		s.handleWrite(conn, writeChan, closeChan)
	})

	r := stream.NewReader(conn)

	for {
		select {
		case <-s.done:
			return
		default:
		}

		logger.Info("handleConnection1")
		size, err := r.ReadUint32()
		logger.Info("handleConnection2")
		if err != nil {

			// 健康检查导致太多关闭，所以不输出日志
			//logger.Error(fmt.Sprintf("ReadUint32 failed:%s", err))

			return
		}

		if size < 4 {
			logger.Error("invalid size", size)
			return
		}

		payload := make([]byte, size)
		err = r.ReadBytes(payload)
		if err != nil {

			// 健康检查导致太多关闭，所以不输出日志
			//logger.Error(fmt.Sprintf("ReadBytes failed:%s", err))

			return
		}

		requestID := binary.BigEndian.Uint64(payload)
		cmd := server.Cmd(binary.BigEndian.Uint32(payload[8:]))

		logger.Info(requestID, cmd)

		switch cmd {
		case server.SetCmd:

			p := server.SetParam{}
			err = gob.FromBytes(payload[12:], &p)
			if err != nil {
				logger.Info(err)
				break
			}

			logger.Info("kv", string(p.Key), string(p.Value))

			err = s.Set(p.Key, p.Value)
			if err == nil {
				writeChan <- server.MakePacket(requestID, server.SetRespCmd, nil)
			}

		case server.GetCmd:
			p := server.GetParam{}
			err = gob.FromBytes(payload[12:], &p)
			var (
				valueBytes, respBytes []byte
			)
			valueBytes, err = s.Get(p.Key)
			if err == nil {
				resp := server.GetResp{Resp: valueBytes}
				respBytes, err = gob.ToBytes(resp)
				if err == nil {
					writeChan <- server.MakePacket(requestID, server.GetRespCmd, respBytes)
				}

			}

		case server.DeleteCmd:
			err = s.Delete(nil)
		case server.DumpCmd:
			_, err = s.Dump()
		case server.JoinCmd:
			p := server.JoinParam{}
			err = gob.FromBytes(payload[12:], &p)
			logger.Info(p.RaftAddr)
			err = s.Join(p.RaftAddr, p.RaftAddr)
			if err == nil {
				writeChan <- server.MakePacket(requestID, server.JoinRespCmd, nil)
			}
		}

		if err != nil {
			logger.Info("MakePacket")
			writeChan <- server.MakePacket(requestID, server.ErrCmd, []byte(err.Error()))
		}
	}
}

func (s *Server) handleWrite(conn net.Conn, writeChann chan []byte, closeChan chan bool) {
	w := stream.NewWriterWithTimeout(conn, DefaultWriteTimeout)
	for {
		select {
		case <-s.done:
			return
		case <-closeChan:
			return
		case bytes := <-writeChann:

			// logger.Debug("writeChann fired")
			if bytes == nil {
				return
			}

			logger.Info("WriteBytes called", bytes)
			_, err := w.Write(bytes)
			if err != nil {
				s.closeConnection(conn, closeChan)
				return
			}
		}
	}
}

// Set do set
func (s *Server) Set(key []byte, value []byte) error {
	return s.store.Set(string(key), string(value))
}

// Delete do delete
func (s *Server) Delete(key []byte) error {

	return nil
}

// Get do get
func (s *Server) Get(key []byte) ([]byte, error) {
	data, _ := s.store.Get(string(key))
	return []byte(data), nil
}

// Dump to dump
func (s *Server) Dump() (map[string][]byte, error) {

	return nil, nil
}

// Join to join
func (s *Server) Join(raftAddr, nodeID string) error {

	return s.store.Join(nodeID, raftAddr)
}

func (s *Server) closeConnection(conn net.Conn, closeChan chan bool) {
	err := conn.Close()
	if err != nil {
		return
	}
	close(closeChan)
}
