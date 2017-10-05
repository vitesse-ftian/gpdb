package main

import (
	"log"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pb "gp_upgrade/idl"
)

const (
	// todo generalize to any host
	address = "localhost:6416"
)

func main() {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewCommandListenerClient(conn)

	reply, err := client.TransmitState(context.Background(), &pb.TransmitStateRequest{Name: "proof of concept"})

	if err != nil {
		log.Fatalf("could not start upgrade: %v", err)
	}
	log.Printf("Command Listener responded: %s", reply.Message)
}
