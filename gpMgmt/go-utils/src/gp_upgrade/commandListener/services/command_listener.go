//go:generate protoc -I ../idl --go_out=plugins=grpc:../idl ../idl/idl.proto

package services

import (
	"golang.org/x/net/context"
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"
)

type commandListenerImpl struct{}

func NewCommandListener() pb.CommandListenerServer {
	return &commandListenerImpl{}
}

func (s *commandListenerImpl) CheckUpgradeStatus(ctx context.Context, in *pb.CheckUpgradeStatusRequest) (*pb.CheckUpgradeStatusReply, error) {
	cmd := "ps auxx | grep pg_upgrade"

	output, err := utils.System.ExecCmdOutput("bash", "-c", cmd)
	if err != nil {
		return nil, err
	}
	return &pb.CheckUpgradeStatusReply{ProcessList: string(output)}, nil
}

func (s *commandListenerImpl) CheckDiskUsage(ctx context.Context, in *pb.CheckDiskUsageRequest) (*pb.CheckDiskUsageReply, error) {
	cmd := "df -h %s"
	output, err := utils.System.ExecCmdOutput("bash", "-c", cmd)
	if err != nil {
		return nil, err
	}
	strOutput := string(output)
	return &pb.CheckDiskUsageReply{FilesystemUsageList: strOutput}, nil
}
