//go:generate protoc -I ../idl --go_out=plugins=grpc:../idl ../idl/idl.proto

package services

import (
	"fmt"
	"golang.org/x/net/context"
	pb "gp_upgrade/idl"
	"os/exec"
)

type commandListenerImpl struct {
	reply string
}

func NewCommandListener(result string) pb.CommandListenerServer {
	return &commandListenerImpl{reply: result}
}

func (s *commandListenerImpl) TransmitState(ctx context.Context, in *pb.TransmitStateRequest) (*pb.TransmitStateReply, error) {
	fmt.Println("replying to message: " + in.Name)
	return &pb.TransmitStateReply{Message: "Finished echo state request: " + in.Name + " " + s.reply}, nil
}

func (s *commandListenerImpl) CheckUpgradeStatus(ctx context.Context, in *pb.CheckUpgradeStatusRequest) (*pb.CheckUpgradeStatusReply, error) {
	commandError := ""
	cmd := "ps auxx | grep pg_upgrade"
	output, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		fmt.Println("error on server side " + err.Error())
		commandError = err.Error()
	}
	fmt.Println("replying to check upgrade status request - " + string(output) + " - blank?")
	return &pb.CheckUpgradeStatusReply{Status: string(output), Error: commandError}, nil
}
