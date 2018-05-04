package utils

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

// GetRPCPort returns port for listening by rpc server
func GetRPCPort() int {
	p, ok := os.LookupEnv("RPC_PORT")
	defaultPort := 5001
	if ok {
		pi, err := strconv.Atoi(p)
		if err != nil {
			log.Printf("UTILS: Error on parsing RPC_PORT: %v", err)
			return defaultPort
		}
		return pi
	}
	return defaultPort
}

// GetIPAddress return local ip address
func GetIPAddress() string {

	conn, err := net.Dial("udp", "8.8.8.8:80")

	defer conn.Close()
	if err != nil {
		log.Printf("UTILS: failed to get ip addrres")
		return ""
	}
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

// RunRPC runs rpc listener binded to specefic object
func RunRPC(nameToRegister string, bindTo interface{}, port int, running *bool, rpcListener **net.Listener) {
	rpcServer := rpc.NewServer()
	rpcServer.RegisterName(nameToRegister, bindTo)

	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	*running = true
	for *running {
		l, e := net.Listen("tcp", fmt.Sprintf(":%d", port))
		*rpcListener = &l
		if e != nil {
			log.Fatalf("RPC: there was an error in listening for http connection on port %v: %v", port, e)
		}
		log.Printf("RPC: Started listening for new http connections in endpoint %s:%v", GetIPAddress(), port)
		err := http.Serve(l, nil)
		if err != nil {
			log.Println("RPC: Error serving connection.")
			continue
		}

		log.Println("RPC: Serving new connection.")
	}
	log.Printf("RPC: rpc server stopped")
}

// GetRemoteClient - returns rpc client connected to endpoint
func GetRemoteClient(endpoint string) (*rpc.Client, bool) {

	c := make(chan error, 1)
	var client *rpc.Client
	var err error
	go func() {
		client, err = rpc.DialHTTP("tcp", endpoint)
		c <- err
	}()

	select {
	case err := <-c:
		if err != nil {
			return nil, false
		}
		return client, true
	case <-time.After(time.Second * 3):
		return nil, false
	}

}
