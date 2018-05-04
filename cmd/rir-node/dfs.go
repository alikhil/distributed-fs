package main

import (
	"errors"
	"fmt"
	"github.com/alikhil/distributed-fs/internals/utils"
	"log"
	"sync/atomic"
	"time"
)

type DistributedFileSystem struct {
	RemoteInterface *RemoteFS
}

// CloseConnections - stops all connectedBefore to the master peers
func (dfs *DistributedFileSystem) CloseConnections() {
	for _, node := range dfs.RemoteInterface.Nodes {
		if node.ConStatus == Connected {
			log.Printf("Master: sending close command to %s", *node.Endpoint)
			err := node.Peer.Close()
			if err != nil {
				log.Printf("Master: failed to close peer: %v", err)
			}
		}
	}
}

type ConnectionStatus byte

const (
	Unknown ConnectionStatus = iota
	Connected
	Disconnected
)

type Node struct {
	Endpoint  *string
	Peer      *PeerIO
	ConStatus ConnectionStatus
}

type RemoteFS struct {
	Nodes                  []*Node
	PeersCount             int
	HealthCheckerIsRunnnig bool
	HealthCheckerTicker    *time.Ticker
	ReadyToUse             bool
}

// type IO interface {
// 	ReadBytes(file string, offset, count int32) (data []byte, ok bool)
// 	WriteBytes(file string, offset int32, bytes *[]byte) (ok bool)
// 	CreateFile(file string) (ok bool)
// 	FileExists(file string) bool
// 	DeleteFile(file string) (ok bool)
// }

func (rfs *RemoteFS) FileExists(filename *string, exists *bool) error {
	if !rfs.ReadyToUse {
		return errors.New("master cannot be used as distributed FS yet. wait untill peers will be connected")
	}
	res := make(chan bool, 3)
	var executed int32 = 0

	for _, node := range rfs.Nodes {
		if node.ConStatus == Connected {
			go func() {
				exists, err := node.Peer.FileExists(filename)
				if err == nil {
					res <- exists
				} else {
					log.Printf("Master: failed to check file(%s) existance: %v", *filename, err)
				}
				atomic.AddInt32(&executed, 1)

			}()
		} else {
			atomic.AddInt32(&executed, 1)
		}
	}

	// TODO: add error returning in case if we can not run file exist in peers
	go func() {
		// if no one of the above gorutines executed nothing will be returned
		// so we wait until last gorutine exetutes and push false to channel
		for executed < int32(rfs.PeersCount) {
		}
		res <- false
	}()

	*exists = <-res
	return nil
}

func (rfs *RemoteFS) AddPeer(peerEndpoint *string, ok *bool) error {
	connectedBefore := len(rfs.Nodes)
	if connectedBefore >= rfs.PeersCount {
		*ok = false
		return fmt.Errorf("there is already %v peers connectedBefore. cannot add more :(", connectedBefore)
	}

	for _, node := range rfs.Nodes {
		if *node.Endpoint == *peerEndpoint {
			// return errors.New("you are already connectedBefore")
			*ok = true
			return nil
		}
	}

	rfs.Nodes = append(rfs.Nodes, &Node{Endpoint: peerEndpoint})
	log.Printf("RPC: peer with endpoint %v connectedBefore; peers: %v/%v", *peerEndpoint, connectedBefore+1, rfs.PeersCount)
	*ok = true

	if connectedBefore == 0 {
		go runHealthChecker(rfs)
	}

	if connectedBefore+1 == rfs.PeersCount {
		log.Printf("Master: needed number of peers connected. Distributed file system ready to use.")
		rfs.ReadyToUse = true
	}
	return nil
}

func runHealthChecker(rfs *RemoteFS) {
	if rfs.HealthCheckerIsRunnnig {
		log.Printf("Health: hey healthchecker is already runnung")
		return
	}

	rfs.HealthCheckerTicker = time.NewTicker(time.Millisecond * 1000)
	rfs.HealthCheckerIsRunnnig = true

	for range rfs.HealthCheckerTicker.C {
		for _, node := range rfs.Nodes {
			if node.Peer == nil {
				client, ok := utils.GetRemoteClient(*node.Endpoint)
				if !ok {
					if node.ConStatus != Disconnected {
						log.Printf("Health: cannot establish rpc connection with peer %s", *node.Endpoint)
					}
					node.ConStatus = Disconnected
					continue
				}
				node.Peer = &PeerIO{client: client}
			}
			err := node.Peer.Ping()
			if err != nil {
				if node.ConStatus != Disconnected {
					log.Printf("Health: ping failed with peer %v", *node.Endpoint)
				}
				node.ConStatus = Disconnected
				node.Peer = nil
				continue
			}
			if node.ConStatus == Disconnected {
				log.Printf("Health: connection with peer %s is restored", *node.Endpoint)
			}
			node.ConStatus = Connected

		}
	}
}
