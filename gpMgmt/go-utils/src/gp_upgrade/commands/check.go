package commands

import (
	"gp_upgrade/config"

	"gp_upgrade/db"

	"github.com/pkg/errors"
	"gp_upgrade/utils"
)

type CheckCommand struct {
	ObjectCount ObjectCountCommand
	GPDBVersion CheckVersionCommand
	MasterHost  string
	MasterPort  int
}

func NewCheckCommand(host string, port int) CheckCommand {
	return CheckCommand{
		MasterHost: host,
		MasterPort: port,
	}
}

func (cmd CheckCommand) Execute([]string) error {
	dbConn := db.NewDBConn(cmd.MasterHost, cmd.MasterPort, "template1")
	return cmd.execute(dbConn, config.NewWriter())
}

func (cmd CheckCommand) execute(dbConnector db.Connector, writer config.Store) error {

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
