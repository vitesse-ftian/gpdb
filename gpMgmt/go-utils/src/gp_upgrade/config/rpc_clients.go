package config

import (
	"fmt"
	"google.golang.org/grpc"
	pb "gp_upgrade/idl"
)

const (
	// todo generalize to any host
	// todo de-duplicate the use of this port in monitor.go
	port = "6416"
)

type RPCClients struct{}

func (helper RPCClients) GetRPCClients() []pb.CommandListenerClient {
	reader := Reader{}
	hostnames := reader.GetHostnames()
	var clients []pb.CommandListenerClient
	for i := 0; i < len(hostnames); i++ {
		conn, err := grpc.Dial(hostnames[i]+":"+port, grpc.WithInsecure())
		if err == nil {
			clients = append(clients, pb.NewCommandListenerClient(conn))
			defer conn.Close()
		} else {
			fmt.Println("ERROR: couldn't get gRPC conn to " + hostnames[i])
		}
	}
	return clients
}
