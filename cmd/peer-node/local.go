package main

import (
	"fmt"
	"github.com/alikhil/TBMS/internals/io"
	"github.com/alikhil/distributed-fs/internals/utils"

	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
)

type localFS struct {
	localIO      *io.LocalIO
	isRPCRunning bool
	rpcListener  *net.Listener
	dbDir        *string
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

func preparePath(fs *localFS, fname *string) (string, error) {
	if filepath.IsAbs(*fname) {
		return "", fmt.Errorf("path %s is absolute. use only relative paths", *fname)
	}

	if strings.Contains(*fname, "/") {
		return "", fmt.Errorf("path contains directories. dfs does not support directories")
	}
	return filepath.Abs(filepath.Join(*fs.dbDir, *fname))
}

func checkExistance(fullpath string) bool {
	if _, err := os.Stat(fullpath); os.IsNotExist(err) {
		return false
	}
	return true
}

func (fs *localFS) FileExists(fname *string, res *bool) error {

	log.Printf("Peer: recieved file exists(%s) request", *fname)

	filename, err := preparePath(fs, fname)
	if err != nil {
		return err
	}

	*res = checkExistance(filename)

	return nil
}

func (fs *localFS) CreateFile(fname *string, res *bool) error {
	log.Printf("Peer: recieved create file(%s) request", *fname)

	filename, err := preparePath(fs, fname)
	if err != nil {
		return err
	}

	_, err = os.Create(filename)
	*res = err == nil
	return err
}

func (fs *localFS) DeleteFile(fname *string, res *bool) error {
	log.Printf("Peer: recieved delete file(%s) request", *fname)

	filename, err := preparePath(fs, fname)

	if err != nil {
		return err
	}

	if checkExistance(filename) {
		os.Remove(filename)
		*res = true
		return nil
	}
	*res = false
	return nil
}

func (fs *localFS) ReadBytes(readArgs *utils.IOReadArgs, data *[]byte) error {

	log.Printf("Peer: recieved read bytes from file(%s) request", *readArgs.Filename)

	fullpath, err := preparePath(fs, readArgs.Filename)
	if err != nil {
		return err
	}

	if !checkExistance(fullpath) {
		return os.ErrNotExist
	}

	file, err := os.Open(fullpath)
	defer file.Close()

	if err != nil {
		return err
	}
	*data = make([]byte, readArgs.Count, readArgs.Count)
	var _, er = file.ReadAt(*data, int64(readArgs.Offset))
	if er != nil {
		return err
	}
	return nil
}

func (fs *localFS) WriteBytes(writeArgs *utils.IOWriteArgs, res *bool) error {
	log.Printf("Peer: recieved write bytes to file(%s) request", *writeArgs.Filename)

	fullpath, err := preparePath(fs, writeArgs.Filename)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(fullpath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	_, err = file.WriteAt(*writeArgs.Data, int64(writeArgs.Offset))
	*res = err == nil
	return err

}
