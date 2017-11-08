package services

import (
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"gp_upgrade/config"
	"gp_upgrade/db"
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"
)

func NewCliToHubListener() pb.CliToHubServer {
	return &cliToHubListenerImpl{}
}

var (
	CreateConfigFile = CreateConfigurationFile
)

type cliToHubListenerImpl struct{}

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

func (s *cliToHubListenerImpl) CheckConfig(ctx context.Context,
	in *pb.CheckConfigRequest) (*pb.CheckConfigReply, error) {

	dbConn := db.NewDBConn("localhost", int(in.DbPort), "template1")
	err := CreateConfigFile(dbConn, config.NewWriter())
	replyString := "All good"
	if err != nil {
		replyString = err.Error()
	}
	reply := &pb.CheckConfigReply{ConfigStatus: replyString}
	return reply, nil
}

func CreateConfigurationFile(dbConnector db.Connector, writer config.Store) error {

	err := dbConnector.Connect()
	if err != nil {
		return utils.DatabaseConnectionError{Parent: err}
	}

	defer dbConnector.Close()

	rows, err := dbConnector.GetConn().Query(`select dbid, content, role, preferred_role,
	mode, status, port, hostname, address, replication_port, fsefsoid, fsedbid, fselocation
	from gp_segment_configuration, pg_filespace_entry
	where fsedbid = dbid`)

	if err != nil {
		return errors.New(err.Error())
	}
	defer rows.Close()

	err = writer.Load(rows)
	if err != nil {
		return errors.New(err.Error())
	}

	err = writer.Write()
	if err != nil {
		return errors.New(err.Error())
	}
	return nil
}
