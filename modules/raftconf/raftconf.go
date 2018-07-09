package raftconf

import (
	"math/rand"
	"qpush/modules/logger"
	cimpl "raftkv/client/impl"
	"sync"
	"time"
)

var (
	cache map[string]string
	lock  sync.RWMutex
)

// Init sets addresses, caller should not refer to addrs any more
func Init(addrs []string) {
	cache = make(map[string]string)
	waitCh := make(chan bool)
	go syncData(addrs, waitCh)
	<-waitCh
}

// Get returns the conf for key
func Get(key string) string {

	lock.RLock()
	defer lock.RUnlock()

	value, ok := cache[key]
	if ok {
		return value
	}

	return ""
}

func syncData(addrs []string, waitCh chan bool) {
	for {
		rand.Shuffle(len(addrs), func(i, j int) {
			addrs[i], addrs[j] = addrs[j], addrs[i]
		})

		syncDataOnce(addrs[0])
		if waitCh != nil {
			close(waitCh)
			waitCh = nil
		}

		time.Sleep(time.Second * 10)

	}
}

func syncDataOnce(addr string) {
	agent := cimpl.Agent{}
	conn, err := agent.Dial(addr)
	if err != nil {
		logger.Error("dial raftkv fail", err)
		time.Sleep(time.Second)
		return
	}
	defer conn.Close()
	m, err := conn.Dump()
	if err != nil {
		logger.Error("dump raftkv fail", err)
		time.Sleep(time.Second)
		return
	}
	lock.Lock()
	defer lock.Unlock()
	cache = m
}
