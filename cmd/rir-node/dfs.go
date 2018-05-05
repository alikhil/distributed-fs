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
	FileToRecordSize       *map[string]int32
	ReadyToUse             bool
}

var ErrNotReady = errors.New("master cannot be used as distributed FS yet. wait untill peers will be connected")
var ErrFileRecordSizeMapNotInited = errors.New("map from file to record size not set")

// InitRecordMappings - should be called before any read and write operation
func (rfs *RemoteFS) InitRecordMappings(fileToRecordLength *map[string]int32, ok *bool) error {
	rfs.FileToRecordSize = fileToRecordLength
	*ok = true
	return nil
}

func (rfs *RemoteFS) WriteBytes(writeArgs *utils.IOWriteArgs, ok *bool) error {
	if !rfs.ReadyToUse {
		return ErrNotReady
	}

	if rfs.FileToRecordSize == nil {
		return ErrFileRecordSizeMapNotInited
	}

	recordSize := (*rfs.FileToRecordSize)[*writeArgs.Filename]
	firstID := writeArgs.Offset/recordSize + 1
	lastID := firstID + int32(len(*writeArgs.Data))/recordSize - 1
	pcnt := int32(rfs.PeersCount)

	offset := writeArgs.Offset
	off := int32(0)
	errs := make(chan error, pcnt)

	var executed int32 = 0

	for id := firstID; id <= lastID; id++ {
		peerID := id % pcnt
		go func(off, offset int32) {
			data := (*writeArgs.Data)[off : off+recordSize]
			if rfs.Nodes[peerID].ConStatus == Connected {
				errs <- rfs.Nodes[peerID].Peer.WriteBytes(writeArgs.Filename, offset, &data)
			} else {
				errs <- fmt.Errorf("one of peers(%v) is disconnected; we can not update all wr")
			}
			atomic.AddInt32(&executed, 1)
		}(off, offset)
		offset += recordSize
		off += recordSize
	}

	go func() {
		for executed < lastID-firstID+1 {
		}
		errs <- nil
	}()

	err := <-errs
	*ok = err == nil
	return err
}

func (rfs *RemoteFS) ReadBytes(readArgs *utils.IOReadArgs, data *[]byte) error {
	if !rfs.ReadyToUse {
		return ErrNotReady
	}

	if rfs.FileToRecordSize == nil {
		return ErrFileRecordSizeMapNotInited
	}
	results := make([]*[]byte, rfs.PeersCount)
	errs := make(chan error, rfs.PeersCount)
	var executed int32 = 0

	for i, node := range rfs.Nodes {
		if node.ConStatus == Connected {
			go func() {
				var err error
				results[i], err = node.Peer.ReadBytes(readArgs)
				if err != nil {
					errs <- err
				}
				atomic.AddInt32(&executed, 1)
			}()
		} else {
			errs <- fmt.Errorf("one of peers(%s) is disconnected; failed to delete file from all the peers", *node.Endpoint)
			atomic.AddInt32(&executed, 1)
		}
	}

	go func() {
		for executed < int32(rfs.PeersCount) {
		}
		errs <- nil
	}()

	err := <-errs
	if err != nil {
		return err
	}
	recordSize := (*rfs.FileToRecordSize)[*readArgs.Filename]
	firstID := readArgs.Offset/recordSize + 1
	lastID := firstID + readArgs.Count/recordSize - 1
	pcnt := int32(rfs.PeersCount)

	resultArray := make([]byte, 0, readArgs.Count)
	off := int32(0)
	for id := firstID; id <= lastID; id++ {
		peerID := id % pcnt
		resultArray = append(resultArray, (*results[peerID])[off:off+recordSize]...)
		off = off + recordSize
	}
	*data = resultArray
	return nil
}

func (rfs *RemoteFS) DeleteFile(filename *string, res *bool) error {
	if !rfs.ReadyToUse {
		return ErrNotReady
	}
	errs := make(chan error, rfs.PeersCount)
	var executed int32 = 0

	for _, node := range rfs.Nodes {
		if node.ConStatus == Connected {
			go func() {
				err := node.Peer.DeleteFile(filename)
				if err != nil {
					errs <- err
					atomic.AddInt32(&executed, 1)
					log.Printf("Master: failed to delete file(%s): %v", *filename, err)
				}
			}()
		} else {
			errs <- fmt.Errorf("one of peers(%s) is disconnected; failed to delete file from all the peers", *node.Endpoint)
			atomic.AddInt32(&executed, 1)
		}
	}

	go func() {
		for executed < int32(rfs.PeersCount) {
		}
		errs <- nil
	}()

	err := <-errs
	*res = err != nil

	return err
}

func (rfs *RemoteFS) FileExists(filename *string, exists *bool) error {
	if !rfs.ReadyToUse {
		return ErrNotReady
	}
	res := make(chan bool, rfs.PeersCount)
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
			log.Printf("Master: cannot check file(%s) existance in peer %s, since it's disconnected", *filename, *node.Endpoint)
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
		allAreOk := true
		for _, node := range rfs.Nodes {
			if node.Peer == nil {
				client, ok := utils.GetRemoteClient(*node.Endpoint)
				if !ok {
					if node.ConStatus != Disconnected {
						log.Printf("Health: cannot establish rpc connection with peer %s", *node.Endpoint)
					}
					node.ConStatus = Disconnected
					allAreOk = false
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
				allAreOk = false
				continue
			}
			if node.ConStatus == Disconnected {
				log.Printf("Health: connection with peer %s is restored", *node.Endpoint)
			}
			node.ConStatus = Connected

		}
		rfs.ReadyToUse = allAreOk
	}
}
