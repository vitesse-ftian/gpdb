package services

import (
	"gp_upgrade/hub/cluster"
	"gp_upgrade/hub/logger"
	pb "gp_upgrade/idl"
)

func NewCliToHubListener(logger logger.LogEntry, pair cluster.PairOperator) pb.CliToHubServer {
	return &cliToHubListenerImpl{logger: logger, clusterPair: pair}
}

type cliToHubListenerImpl struct {
	logger      logger.LogEntry
	clusterPair cluster.PairOperator
}
