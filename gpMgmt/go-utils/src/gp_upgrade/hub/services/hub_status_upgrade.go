package services

import (
	pb "gp_upgrade/idl"

	"golang.org/x/net/context"
)

func (s *cliToHubListenerImpl) StatusUpgrade(ctx context.Context, in *pb.StatusUpgradeRequest) (*pb.StatusUpgradeReply, error) {
	demoStepStatus := &pb.UpgradeStepStatus{
		Step:   pb.UpgradeSteps_CHECK_CONFIG,
		Status: pb.StepStatus_PENDING,
	}
	demoSeginstallStatus := &pb.UpgradeStepStatus{
		Step:   pb.UpgradeSteps_SEGINSTALL,
		Status: pb.StepStatus_PENDING,
	}

	reply := &pb.StatusUpgradeReply{}
	reply.ListOfUpgradeStepStatuses = append(reply.ListOfUpgradeStepStatuses, demoStepStatus, demoSeginstallStatus)
	return reply, nil
}
