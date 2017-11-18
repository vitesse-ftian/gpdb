package commanders

import (
	"context"
	pb "gp_upgrade/idl"

	gpbackupUtils "github.com/greenplum-db/gpbackup/utils"
)

type VersionChecker struct {
	client pb.CliToHubClient
}

func NewVersionChecker(client pb.CliToHubClient) VersionChecker {
	return VersionChecker{
		client: client,
	}
}

func (req VersionChecker) Execute(masterHost string, dbPort int) error {
	logger := gpbackupUtils.GetLogger()
	resp, err := req.client.CheckVersion(context.Background(),
		&pb.CheckVersionRequest{Host: masterHost, DbPort: int32(dbPort)})
	if err != nil {
		logger.Error("ERROR - Unable to connect to hub")
		return err
	}
	if resp.IsVersionCompatible {
		logger.Info("gp_upgrade: Version Compatibility Check [OK]\n")
	} else {
		logger.Info("gp_upgrade: Version Compatibility Check [Failed]\n")
	}
	logger.Info("Check version request is processed.")

	return nil
}
