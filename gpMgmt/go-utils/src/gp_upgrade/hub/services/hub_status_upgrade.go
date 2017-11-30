package services

import (
	"gp_upgrade/hub/configutils"
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"

	gpbackupUtils "github.com/greenplum-db/gpbackup/utils"
	"golang.org/x/net/context"
)

func (s *cliToHubListenerImpl) StatusUpgrade(ctx context.Context, in *pb.StatusUpgradeRequest) (*pb.StatusUpgradeReply, error) {
	gpbackupUtils.GetLogger().Info("starting StatusUpgrade")
	demoStepStatus := &pb.UpgradeStepStatus{
		Step:   pb.UpgradeSteps_CHECK_CONFIG,
		Status: pb.StepStatus_PENDING,
	}
	demoSeginstallStatus := &pb.UpgradeStepStatus{
		Step:   pb.UpgradeSteps_SEGINSTALL,
		Status: pb.StepStatus_PENDING,
	}
	prepareInitStatus, _ := GetPrepareNewClusterConfigStatus()

	reply := &pb.StatusUpgradeReply{}
	reply.ListOfUpgradeStepStatuses = append(reply.ListOfUpgradeStepStatuses, demoStepStatus, demoSeginstallStatus, prepareInitStatus)
	return reply, nil
}

func GetPrepareNewClusterConfigStatus() (*pb.UpgradeStepStatus, error) {
	/* Treat all stat failures as cannot find file. Conceal worse failures atm.*/
	_, err := utils.System.Stat(configutils.GetNewClusterConfigFilePath())

	if err != nil {
		gpbackupUtils.GetLogger().Debug("%v", err)
		return &pb.UpgradeStepStatus{Step: pb.UpgradeSteps_PREPARE_INIT_CLUSTER,
			Status: pb.StepStatus_PENDING}, nil
	}

	return &pb.UpgradeStepStatus{Step: pb.UpgradeSteps_PREPARE_INIT_CLUSTER,
		Status: pb.StepStatus_COMPLETE}, nil
}
