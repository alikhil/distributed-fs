package main

import (
	"flag"
	"fmt"
	"github.com/alikhil/distributed-fs/internals/utils"
	"log"
	"net/rpc"
)

func main() {
	remoteEndpoint := flag.String("endpoint", "10.91.41.109:5001", "endpoint of master node")
	port := flag.Int("port", 5002, "port for rpc connection from master node")
	dbDir := flag.String("dbdir", "peer-data", "directory where all files of the peer will be stored")

	flag.Parse()

	log.Printf("Peer: Connecting to master with endpoint %v", *remoteEndpoint)

	client, ok := utils.GetRemoteClient(*remoteEndpoint)
	if !ok {
		log.Printf("RPC: cannot connect to endpoint %v", remoteEndpoint)
		return
	}

	master := master{client: client}
	err := master.connectAsPeer(*port)
	if err != nil {
		log.Fatalf("RPC: failed to connect as a peer: %v", err)
	}

	fs := localFS{dbDir: dbDir}
	utils.RunRPC("PeerFS", &fs, *port, &fs.isRPCRunning, &fs.rpcListener)
	return
}

type master struct {
	client *rpc.Client
}

func (m *master) connectAsPeer(port int) error {
	endpoint := fmt.Sprintf("%s:%d", utils.GetIPAddress(), port)
	var ok bool
	err := m.client.Call("RemoteIO.AddPeer", &endpoint, &ok)
	return err
}
