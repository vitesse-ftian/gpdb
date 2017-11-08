package main

import (
	"gp_upgrade/hub/services"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "gp_upgrade/idl"

	gpbackupUtils "github.com/greenplum-db/gpbackup/utils"
)

const (
	cliToHubPort = ":7527"
)

// This directory to have the implementation code for the gRPC server to serve
// Minimal CLI command parsing to embrace that booting this binary to run the hub might have some flags like a log dir

func main() {
	gpbackupUtils.InitializeLogging("hub", "")
	logger := gpbackupUtils.GetLogger()
	errorChannel := make(chan error)
	defer close(errorChannel)
	lis, err := net.Listen("tcp", cliToHubPort)
	if err != nil {
		logger.Fatal(err, "failed to listen")
	}

	server := grpc.NewServer()
	myImpl := services.NewCliToHubListener()
	pb.RegisterCliToHubServer(server, myImpl)
	reflection.Register(server)
	go func(myListener net.Listener) {
		if err := server.Serve(myListener); err != nil {
			logger.Fatal(err, "failed to serve", err)
			errorChannel <- err
		}

		close(errorChannel)
	}(lis)

	select {
	case err := <-errorChannel:
		if err != nil {
			logger.Fatal(err, "error during Listening")
		}
	}

}
