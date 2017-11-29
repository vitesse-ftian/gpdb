package commanders

import (
	"context"

	pb "gp_upgrade/idl"

	gpbackupUtils "github.com/greenplum-db/gpbackup/utils"
)

type Upgrader struct {
	client pb.CliToHubClient
}

func NewUpgrader(client pb.CliToHubClient) Upgrader {
	return Upgrader{client: client}
}

func (u Upgrader) ConvertMaster(oldDataDir string, oldBinDir string, newDataDir string, newBinDir string) error {
	logger := gpbackupUtils.GetLogger()
	upgradeConvertMasterRequest := pb.UpgradeConvertMasterRequest{
		OldDataDir: oldDataDir,
		OldBinDir:  oldBinDir,
		NewDataDir: newDataDir,
		NewBinDir:  newBinDir,
	}
	_, err := u.client.UpgradeConvertMaster(context.Background(), &upgradeConvertMasterRequest)
	if err != nil {
		// TODO: Change the logging message?
		logger.Error("ERROR - Unable to connect to hub")
		return err
	}

	logger.Info("Kicked off pg_upgrade request.")
	return nil
}
