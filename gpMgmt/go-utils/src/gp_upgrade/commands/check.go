package commands

import (
	"gp_upgrade/config"

	"gp_upgrade/db"

	"gp_upgrade/utils"
)

type CheckCommand struct {
	ObjectCount ObjectCountCommand  `command:"object-count" alias:"oc" description:"count database objects and numeric objects"`
	GPDBVersion CheckVersionCommand `command:"version" alias:"ver" description:"validate current version is upgradable"`

	MasterHost string
	MasterPort int
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
		return err
	}
	defer rows.Close()

	err = writer.Load(rows)
	if err != nil {
		return err
	}

	err = writer.Write()
	return err
}
