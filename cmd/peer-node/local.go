package main

import (
	"github.com/alikhil/TBMS/internals/io"
	"log"
	"net"
)

type localFS struct {
	localIO      *io.LocalIO
	isRPCRunning bool
	rpcListener  *net.Listener
}

func (*localFS) Ping(a, b *int) error {
	*b = *a
	return nil
}

func (fs *localFS) Close(a, b *int) error {
	log.Printf("RPC: recieved close command; stopping everything...")
	fs.isRPCRunning = false
	(*fs.rpcListener).Close()
	return nil
}
