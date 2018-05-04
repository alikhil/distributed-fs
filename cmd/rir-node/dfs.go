package main

import (
	"fmt"
	"github.com/alikhil/distributed-fs/internals/utils"
	"log"
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
