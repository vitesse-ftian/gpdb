package services

import (
	"log"
	"net"

	"github.com/greenplum-db/gpbackup/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	pb "gp_upgrade/idl"
)

const (
	port = ":6416"
)

type CommandListenerStartCommand struct {
	LogDir string `long:"log-directory" required:"no" default:"/var/log/greenplum" description:"The directory in which logs will be written."`
}

func (cmd CommandListenerStartCommand) execute([]string) (*grpc.Server, chan error, error) {
	errorChannel := make(chan error)

	utils.InitializeLogging("command_listener", cmd.LogDir)
	utils.GetLogger().Info("Starting Command Listener")

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Printf("failed to listen: %v\n", err)
		return nil, errorChannel, err
	}

	server := grpc.NewServer()
	myImpl := NewCommandListener("foo")
	pb.RegisterCommandListenerServer(server, myImpl)
	reflection.Register(server)
	go func(myListener net.Listener) {
		if err := server.Serve(myListener); err != nil {
			utils.GetLogger().Info("failed to serve: %v\n", err)
			log.Printf("failed to serve: %v\n", err)
			errorChannel <- err
		}

		close(errorChannel)
	}(lis)

	return server, errorChannel, err

}

func (cmd CommandListenerStartCommand) Execute([]string) error {
	_, errorChannel, err := cmd.execute(nil)
	if err != nil {
		close(errorChannel)
		return err
	}
	return <-errorChannel
}
