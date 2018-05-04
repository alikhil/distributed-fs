package main

import (
	"net/rpc"
)

type PeerIO struct {
	client *rpc.Client
}

func (peer *PeerIO) Ping() error {
	a := 4
	b := 0
	return peer.client.Call("PeerFS.Ping", &a, &b)
}

func (peer *PeerIO) Close() error {
	var a = 5
	var b = 4
	return peer.client.Call("PeerFS.Close", &a, &b)
}

func (peer *PeerIO) FileExists(fname *string) (bool, error) {
	return false, nil
}
