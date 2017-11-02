package commanders

import (
	"errors"
	"os/exec"
	"strconv"
	"strings"

	gpbackupUtils "github.com/greenplum-db/gpbackup/utils"
)

type Preparer struct{}

func (p Preparer) StartHub() error {
	logger := gpbackupUtils.GetLogger()

	countHubs, err := howManyHubsRunning()
	if err != nil {
		logger.Error("failed to determine if hub already running")
		return err
	}
	if countHubs >= 1 {
		logger.Error("gp_upgrade_hub process already running")
		return errors.New("gp_upgrade_hub process already running")
	}

	//assume that gp_upgrade_hub is on the PATH
	cmd := exec.Command("gp_upgrade_hub")
	cmdErr := cmd.Start()
	if cmdErr != nil {
		logger.Error("gp_upgrade_hub kickoff failed")
		return cmdErr
	}
	logger.Debug("gp_upgrade_hub started")
	return nil
}

func howManyHubsRunning() (int, error) {
	howToLookForHub := "ps -ef | grep -c [g]p_upgrade_hub" // use square brackets to avoid finding yourself in matches
	output, err := exec.Command("bash", "-c", howToLookForHub).Output()
	value, convErr := strconv.Atoi(strings.TrimSpace(string(output)))
	if convErr != nil {
		if err != nil {
			return -1, err
		}
		return -1, convErr
	}

	// let value == 0 through before checking err, for when grep finds nothing and its error-code is 1
	if value >= 0 {
		return value, nil
	}

	// only needed if the command errors, but somehow put a parsable & negative value on stdout
	return -1, err
}
