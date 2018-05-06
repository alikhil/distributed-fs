package utils

import (
	"net/rpc"
)

type DFSClient struct {
	Client *rpc.Client
}

func (dfs *DFSClient) InitRecordMappings(mp *map[string]int32) error {
	ok := false
	return dfs.Client.Call("RemoteIO.InitRecordMappings", mp, &ok)
}

func (dfs *DFSClient) FileExists(fname *string) (ok bool, err error) {
	err = dfs.Client.Call("RemoteIO.FileExists", fname, &ok)
	return
}

func (dfs *DFSClient) DeleteFile(fname *string) error {
	ok := false
	return dfs.Client.Call("RemoteIO.DeleteFile", fname, &ok)
}

func (dfs *DFSClient) ReadBytes(fname *string, offset, count int32) (*[]byte, error) {
	data := make([]byte, count, count)

	err := dfs.Client.Call("RemoteIO.ReadBytes", &IOReadArgs{Offset: offset, Count: count, Filename: fname}, &data)
	return &data, err
}

func (dfs *DFSClient) WriteBytes(fname *string, offset int32, data *[]byte) error {
	ok := false
	return dfs.Client.Call("RemoteIO.WriteBytes", &IOWriteArgs{Offset: offset, Data: data, Filename: fname}, &ok)
}

func (dfs *DFSClient) CreateFile(fname *string) error {
	ok := false
	return dfs.Client.Call("RemoteIO.CreateFile", fname, &ok)
}
