package commands

import (
	"context"
	"fmt"

	pb "gp_upgrade/idl"
	"io"
)

type Hub struct{}

func (h Hub) CheckDiskUsage(clients []pb.CommandListenerClient, writer io.Writer) {
	fmt.Fprint(writer, "CheckDiskUsageCommand Execute")

	var diskUsageResults []string

	for i := 0; i < len(clients); i++ {
		reply, err := clients[i].CheckDiskUsage(context.Background(), &pb.CheckDiskUsageRequest{})
		if err != nil {
			//todo: get hostname from clientconn?
			fmt.Fprint(writer, "Could not get disk usage from: "+err.Error())
		}
		diskUsageResults = append(diskUsageResults, reply.FilesystemUsageList)
	}

	fmt.Fprint(writer, "gp_upgrade: Disk Usage Check [OK]\n")
}
