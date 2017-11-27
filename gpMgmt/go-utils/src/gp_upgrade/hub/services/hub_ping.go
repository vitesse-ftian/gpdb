package services

import (
	gpbackupUtils "github.com/greenplum-db/gpbackup/utils"
	"golang.org/x/net/context"
	pb "gp_upgrade/idl"
)

func (s *cliToHubListenerImpl) Ping(ctx context.Context,
	in *pb.PingRequest) (*pb.PingReply, error) {

	gpbackupUtils.GetLogger().Info("starting Ping")
	return &pb.PingReply{}, nil
}
