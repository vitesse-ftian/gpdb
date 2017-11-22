package commanders

import (
	"context"
	gpbackupUtils "github.com/greenplum-db/gpbackup/utils"
	pb "gp_upgrade/idl"
)

type ObjectCountChecker struct {
	client pb.CliToHubClient
}

func NewObjectCountChecker(client pb.CliToHubClient) ObjectCountChecker {
	return ObjectCountChecker{client: client}
}

func (req ObjectCountChecker) Execute(dbPort int) error {
	logger := gpbackupUtils.GetLogger()
	reply, err := req.client.CheckObjectCount(context.Background(),
		&pb.CheckObjectCountRequest{DbPort: int32(dbPort)})
	if err != nil {
		logger.Error("ERROR - gRPC call to hub failed")
		return err
	}
	//TODO: do we want to report results to the user earlier? Should we make a gRPC call per db?
	for _, count := range reply.ListOfCounts {
		logger.Info("Checking object counts in database: %s", count.DbName)
		logger.Info("Number of AO objects - %d", count.AoCount)
		logger.Info("Number of heap objects - %d", count.HeapCount)
	}
	logger.Info("Check object count request is processed.")
	return nil
}
