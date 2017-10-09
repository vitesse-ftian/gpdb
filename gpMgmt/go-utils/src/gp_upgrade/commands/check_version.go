package commands

import (
	"fmt"

	"io"

	"gp_upgrade/db"
	"os"

	"regexp"

	"gp_upgrade/utils"

	"github.com/cppforlife/go-semi-semantic/version"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type CheckVersionCommand struct {
	MasterHost string
	MasterPort int
}

const (
	MINIMUM_VERSION = "4.3.9.0"
)

func (cmd CheckVersionCommand) Execute([]string) error {
	dbConn := db.NewDBConn(cmd.MasterHost, cmd.MasterPort, "template1")
	return cmd.execute(dbConn, os.Stdout)
}

func (cmd CheckVersionCommand) execute(dbConnector db.Connector, outputWriter io.Writer) error {

	err := dbConnector.Connect()
	if err != nil {
		return utils.DatabaseConnectionError{Parent: err}
	}
	defer dbConnector.Close()

	var connection *sqlx.DB
	connection = dbConnector.GetConn()
	var row string
	err = connection.QueryRow("SELECT version()").Scan(&row)
	if err != nil {
		return errors.New(err.Error())
	}

	re := regexp.MustCompile("Greenplum Database (.*) build")

	versionString := re.FindStringSubmatch(row)[1]
	versionObject, err := version.NewVersionFromString(versionString)
	if err != nil {
		return errors.New(err.Error())
	}

	if versionObject.IsGt(version.MustNewVersionFromString(MINIMUM_VERSION)) {
		fmt.Fprint(outputWriter, "gp_upgrade: Version Compatibility Check [OK]\n")
	} else {
		fmt.Fprint(outputWriter, "gp_upgrade: Version Compatibility Check [Failed]\n")
	}
	return nil
}
