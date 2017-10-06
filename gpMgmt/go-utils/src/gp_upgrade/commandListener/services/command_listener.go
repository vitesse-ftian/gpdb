//go:generate protoc -I ../idl --go_out=plugins=grpc:../idl ../idl/idl.proto

package services

import (
	"golang.org/x/net/context"
	pb "gp_upgrade/idl"
)

type commandListenerImpl struct {
	reply string
}

func NewCommandListener(result string) pb.CommandListenerServer {
	return &commandListenerImpl{reply: result}
}

func (s *commandListenerImpl) TransmitState(ctx context.Context, in *pb.TransmitStateRequest) (*pb.TransmitStateReply, error) {
	return &pb.TransmitStateReply{Message: "Finished echo state request: " + in.Name + " " + s.reply}, nil
}
