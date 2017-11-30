package commanders

import (
	"context"
	"fmt"

	pb "gp_upgrade/idl"

	gpbackupUtils "github.com/greenplum-db/gpbackup/utils"
)

type Reporter struct {
	client pb.CliToHubClient
}

var UpgradeStepsMessage = map[pb.UpgradeSteps]string{
	pb.UpgradeSteps_UNKNOWN_STEP:         "- Unknown step",
	pb.UpgradeSteps_CHECK_CONFIG:         "- Configuration Check",
	pb.UpgradeSteps_SEGINSTALL:           "- Install binaries on segments",
	pb.UpgradeSteps_PREPARE_INIT_CLUSTER: "- Initialize upgrade target cluster",
}

func NewReporter(client pb.CliToHubClient) Reporter {
	return Reporter{client: client}
}

func (r *Reporter) OverallUpgradeStatus() error {
	logger := gpbackupUtils.GetLogger()
	reply, err := r.client.StatusUpgrade(context.Background(), &pb.StatusUpgradeRequest{})
	if err != nil {
		logger.Error("ERROR - Unable to connect to hub")
		return err
	}

	for i := 0; i < len(reply.ListOfUpgradeStepStatuses); i++ {
		upgradeStepStatus := reply.ListOfUpgradeStepStatuses[i]
		reportString := fmt.Sprintf("%v %s", upgradeStepStatus.Status,
			UpgradeStepsMessage[upgradeStepStatus.Step])
		logger.Info(reportString)
	}

	logger.Info("PENDING - Validate compatible versions for upgrade")
	logger.Info("PENDING - Shutdown cluster")
	logger.Info("PENDING - Master server upgrade")
	logger.Info("PENDING - Master OID file shared with segments")
	logger.Info("PENDING - Primary segment upgrade")
	logger.Info("PENDING - Validate cluster start")
	logger.Info("PENDING - Adjust upgrade cluster ports")

	return nil
}
