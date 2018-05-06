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

func (dfs *DFSClient) FileExists(fname string) bool {
	ok := false
	err := dfs.Client.Call("RemoteIO.FileExists", &fname, &ok)
	return ok && err == nil
}

func (dfs *DFSClient) DeleteFile(fname string) bool {
	ok := false
	return dfs.Client.Call("RemoteIO.DeleteFile", &fname, &ok) == nil && ok
}

func (dfs *DFSClient) ReadBytes(fname string, offset, count int32) ([]byte, bool) {
	data := make([]byte, count, count)

	err := dfs.Client.Call("RemoteIO.ReadBytes", &IOReadArgs{Offset: offset, Count: count, Filename: &fname}, &data)
	return data, err == nil
}

func (dfs *DFSClient) WriteBytes(fname string, offset int32, data *[]byte) bool {
	ok := false
	return dfs.Client.Call("RemoteIO.WriteBytes", &IOWriteArgs{Offset: offset, Data: data, Filename: &fname}, &ok) == nil && ok
}

func (dfs *DFSClient) CreateFile(fname string) bool {
	ok := false
	return dfs.Client.Call("RemoteIO.CreateFile", &fname, &ok) == nil && ok
}
