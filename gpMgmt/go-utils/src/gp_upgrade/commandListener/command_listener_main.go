package main

import (
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	pb "gp_upgrade/idl"
	"gp_upgrade/services"
)

const (
	port = ":6416"
)

func main() {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	myServer := grpc.NewServer()
	myImpl := services.NewCommandListener("foo")
	pb.RegisterCommandListenerServer(myServer, myImpl)
	reflection.Register(myServer)
	if err := myServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}