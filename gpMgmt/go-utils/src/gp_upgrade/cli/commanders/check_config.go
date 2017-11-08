package commanders

import (
	"context"
	gpbackupUtils "github.com/greenplum-db/gpbackup/utils"
	pb "gp_upgrade/idl"
)

type CheckConfigRequest struct {
	client pb.CliToHubClient
}

func NewCheckConfigRequest(client pb.CliToHubClient) CheckConfigRequest {
	return CheckConfigRequest{
		client: client,
	}
}

func (req CheckConfigRequest) Execute(dbPort int) error {
	logger := gpbackupUtils.GetLogger()
	_, err := req.client.CheckConfig(context.Background(),
		&pb.CheckConfigRequest{DbPort: int32(dbPort)})
	if err != nil {
		logger.Error("ERROR - Unable to connect to hub")
		return err
	}
	logger.Info("Check config request is processed.")
	return nil
}
