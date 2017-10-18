package commands

import (
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"gp_upgrade/config"
	pb "gp_upgrade/idl"
	"io"
	"os"
)

type CheckDiskUsageCommand struct{}

func NewDiskUsageCommand() CheckDiskUsageCommand {
	return CheckDiskUsageCommand{}
}

func (cmd CheckDiskUsageCommand) Execute() {
	fmt.Println("CheckDiskUsageCommand Execute")
	reader := config.Reader{}
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
	cmd.execute(os.Stdout, clients)
}

func (cmd CheckDiskUsageCommand) execute(outputWriter io.Writer, clients []pb.CommandListenerClient) {
	var diskUsageResults []string

	for i := 0; i < len(clients); i++ {
		reply, err := clients[i].CheckDiskUsage(context.Background(), &pb.CheckDiskUsageRequest{})
		if err != nil {
			//todo: get hostname from clientconn?
			fmt.Println("Could not get disk usage from: " + err.Error())
		}
		diskUsageResults = append(diskUsageResults, reply.FilesystemUsageList)
	}
	fmt.Fprint(outputWriter, "gp_upgrade: Disk Usage Check [OK]\n")
}
