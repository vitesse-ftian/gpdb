package services

import (
	"fmt"
	gpbackupUtils "github.com/greenplum-db/gpbackup/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"gp_upgrade/hub/configutils"
	pb "gp_upgrade/idl"
)

const (
	// todo generalize to any host
	address               = "localhost"
	port                  = "6416"
	diskUsageWarningLimit = 80
)

func (s *cliToHubListenerImpl) CheckDiskUsage(ctx context.Context,
	in *pb.CheckDiskUsageRequest) (*pb.CheckDiskUsageReply, error) {

	var replyMessages []string
	reader := configutils.Reader{}
	hostnames := reader.GetHostnames()
	var clients []configutils.ClientAndHostname
	for i := 0; i < len(hostnames); i++ {
		conn, err := grpc.Dial(hostnames[i]+":"+port, grpc.WithInsecure())
		if err == nil {
			clients = append(clients, configutils.ClientAndHostname{Client: pb.NewCommandListenerClient(conn), Hostname: hostnames[i]})
			defer conn.Close()
		} else {
			replyMessages = append(replyMessages, "ERROR: couldn't get gRPC conn to "+hostnames[i])
		}
	}
	replyMessages = append(replyMessages, GetDiskUsageFromSegmentHosts(clients)...)

	return &pb.CheckDiskUsageReply{SegmentFileSysUsage: replyMessages}, nil
}

func GetDiskUsageFromSegmentHosts(clients []configutils.ClientAndHostname) []string {
	replyMessages := []string{}
	for i := 0; i < len(clients); i++ {
		reply, err := clients[i].Client.CheckDiskUsageOnAgents(context.Background(),
			&pb.CheckDiskUsageRequestToAgent{})
		if err != nil {
			replyMessages = append(replyMessages, "Could not get disk usage from: "+clients[i].Hostname)
			gpbackupUtils.GetLogger().Error(err.Error())
			continue
		}
		//todo: get hostname from clientconn?
		foundAnyTooFull := false
		for _, line := range reply.ListOfFileSysUsage {
			if line.Usage >= diskUsageWarningLimit {
				replyMessages = append(replyMessages, fmt.Sprintf("diskspace check - %s - WARNING %s %.1f use",
					clients[i].Hostname, line.Filesystem, line.Usage))
				foundAnyTooFull = true
			}
		}
		if !foundAnyTooFull {
			//TODO actual hostname instead of hostA
			replyMessages = append(replyMessages, fmt.Sprintf("diskspace check - %s - OK", clients[i].Hostname))
		}
	}

	return replyMessages
}
