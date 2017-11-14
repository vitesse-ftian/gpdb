package services

import (
	"gp_upgrade/config"
	"gp_upgrade/db"
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

func (s *cliToHubListenerImpl) CheckConfig(ctx context.Context,
	in *pb.CheckConfigRequest) (*pb.CheckConfigReply, error) {

	dbConnector := db.NewDBConn("localhost", int(in.DbPort), "template1")
	defer dbConnector.Close()
	err := dbConnector.Connect()
	if err != nil {
		return nil, utils.DatabaseConnectionError{Parent: err}
	}
	databaseHandler := dbConnector.GetConn()

	err = CreateConfigurationFile(databaseHandler, config.NewWriter())
	if err != nil {
		return nil, err
	}
	successReply := &pb.CheckConfigReply{ConfigStatus: "All good"}
	return successReply, nil
}

// public for testing purposes
func CreateConfigurationFile(databaseHandler *sqlx.DB, writer config.Store) error {
	rows, err := databaseHandler.Query(`select dbid, content, role, preferred_role,
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
