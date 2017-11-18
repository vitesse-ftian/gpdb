package services

import (
	"errors"
	"github.com/cppforlife/go-semi-semantic/version"
	"github.com/jmoiron/sqlx"
	"golang.org/x/net/context"
	"gp_upgrade/db"
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"
	"regexp"
)

func (s *cliToHubListenerImpl) CheckVersion(ctx context.Context,
	in *pb.CheckVersionRequest) (*pb.CheckVersionReply, error) {

	dbConnector := db.NewDBConn(in.Host, int(in.DbPort), "template1")
	defer dbConnector.Close()
	err := dbConnector.Connect()
	if err != nil {
		return nil, utils.DatabaseConnectionError{Parent: err}
	}
	databaseHandler := dbConnector.GetConn()
	isVersionCompatible, err := VerifyVersion(databaseHandler)
	if err != nil {
		return nil, errors.New(err.Error())
	}
	return &pb.CheckVersionReply{IsVersionCompatible: isVersionCompatible}, nil
}

func VerifyVersion(dbHandler *sqlx.DB) (bool, error) {
	var row string
	err := dbHandler.Get(&row, VERSION_QUERY)
	if err != nil {
		return false, errors.New(err.Error())
	}
	re := regexp.MustCompile("Greenplum Database (.*) build")
	versionStringResults := re.FindStringSubmatch(row)
	if len(versionStringResults) < 2 {
		return false, errors.New("Didn't get a version string match")
	}
	versionString := versionStringResults[1]
	versionObject, err := version.NewVersionFromString(versionString)
	if err != nil {
		return false, err
	}
	if versionObject.IsGt(version.MustNewVersionFromString(MINIMUM_VERSION)) {
		return true, nil
	}
	return false, nil
}

const (
	VERSION_QUERY   = `SELECT version()`
	MINIMUM_VERSION = "4.3.9.0"
)
