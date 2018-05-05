package main

import (
	"github.com/alikhil/distributed-fs/internals/utils"
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

func (peer *PeerIO) FileExists(fname *string) (result bool, err error) {
	err = peer.client.Call("PeerFS.FileExists", fname, &result)
	return
}

func (peer *PeerIO) DeleteFile(fname *string) error {
	ok := false
	return peer.client.Call("PeerFS.DeleteFile", fname, &ok)
}

func (peer *PeerIO) ReadBytes(readArgs *utils.IOReadArgs) (*[]byte, error) {
	bytes := make([]byte, readArgs.Count)
	err := peer.client.Call("PeerFS.ReadBytes", readArgs, &bytes)
	return &bytes, err
}

func (peer *PeerIO) WriteBytes(filename *string, offset int32, data *[]byte) error {
	args := &utils.IOWriteArgs{Filename: filename, Offset: offset, Data: data}
	ok := true
	err := peer.client.Call("PeerFS.WriteBytes", args, &ok)
	return err
}
