package commanders

import (
	"context"
	gpbackupUtils "github.com/greenplum-db/gpbackup/utils"
	pb "gp_upgrade/idl"
)

type DiskUsageChecker struct {
	client pb.CliToHubClient
}

func NewDiskUsageChecker(client pb.CliToHubClient) DiskUsageChecker {
	return DiskUsageChecker{client: client}
}

func (req DiskUsageChecker) Execute(dbPort int) error {
	logger := gpbackupUtils.GetLogger()
	reply, err := req.client.CheckDiskUsage(context.Background(),
		&pb.CheckDiskUsageRequest{})
	if err != nil {
		logger.Error("ERROR - Unable to connect to hub")
		return err
	}

	//TODO: do we want to report results to the user earlier? Should we make a gRPC call per db?
	for _, segmentFileSysUsage := range reply.SegmentFileSysUsage {
		logger.Info(segmentFileSysUsage)
	}
	logger.Info("Check object count request is processed.")
	return nil
}
