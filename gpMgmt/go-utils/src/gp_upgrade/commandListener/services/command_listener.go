//go:generate protoc -I ../idl --go_out=plugins=grpc:../idl ../idl/idl.proto

package services

import (
	"fmt"
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
	fmt.Println("replying to check upgrade status request - " + string(output) + " - blank?")
	return &pb.CheckUpgradeStatusReply{ProcessList: string(output)}, nil
}
