package main

import (
	"github.com/alikhil/distributed-fs/utils"
	"log"
)

func main() {

	var client, ok = utils.GetRemoteClient("10.91.41.109:5001")
	if !ok {
		log.Printf("Failed to connect to master")
	}
	var dfs = utils.DFSClient{Client: client}
	fname := "testfile"
	// err := dfs.CreateFile(&fname)
	// if err == nil {
	// 	log.Printf("file created %s", fname)
	// } else {
	// 	log.Printf("error occured %v", err)
	// }

	// ok, err := dfs.FileExists(&fname)
	// if err == nil {
	// 	log.Printf("file exists %s - %v", fname, ok)
	// } else {
	// 	log.Printf("error occured %v", err)
	// }

	mp := map[string]int32{
		fname: int32(5),
	}
	err := dfs.InitRecordMappings(&mp)
	if err != nil {
		log.Printf("error occured %v", err)
	} else {
		log.Printf("map set")
	}

	// data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}
	// err = dfs.WriteBytes(&fname, 0, &data)

	// if err != nil {
	// 	log.Printf("error occured %v", err)
	// } else {
	// 	log.Printf("bytes writed to file")
	// }

	// data := []byte{20, 20, 20, 20, 20}
	// err = dfs.WriteBytes(&fname, 5, &data)

	// if err != nil {
	// 	log.Printf("error occured %v", err)
	// } else {
	// 	log.Printf("bytes writed to file")
	// }

	rdata, rerr := dfs.ReadBytes(&fname, 0, 15)

	if rerr != nil {
		log.Printf("error occured: %v", rerr)
	} else {
		log.Printf("bytes read from file %v", rdata)
	}

	// derr := dfs.DeleteFile(&fname)
	// if derr != nil {
	// 	log.Printf("error occured %v", derr)
	// } else {
	// 	log.Printf("file deleted")
	// }

}
