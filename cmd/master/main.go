package main

import (
	"flag"
	"github.com/alikhil/distributed-fs/utils"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

// Implements remote interface for IO
// clients connect to RIR
// RIR uses peers

type masterServer struct {
	running     bool
	rpcListener *net.Listener
	dfs         *DistributedFileSystem
}

func main() {
	peersCount := flag.Int("peers", 3, "numbers of peers in DFS")
	silent := flag.Bool("silent", false, "if true no log will be printed")

	flag.Parse()
	if *silent {
		log.SetOutput(ioutil.Discard)
	}

	mserver := &masterServer{dfs: &DistributedFileSystem{RemoteInterface: &RemoteFS{PeersCount: *peersCount}}}

	handleSignals(mserver)
	utils.RunRPC("RemoteIO", mserver.dfs.RemoteInterface, utils.GetRPCPort(), &mserver.running, &mserver.rpcListener)
}

func handleSignals(server *masterServer) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		s := <-sigc
		log.Printf("Recived signal from keyboard: %s stopping master and peers", s.String())
		server.dfs.CloseConnections()
		server.running = false
		(*server.rpcListener).Close()
	}()
}
