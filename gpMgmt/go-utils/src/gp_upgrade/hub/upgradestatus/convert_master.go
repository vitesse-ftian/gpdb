package upgradestatus

import (
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"

	"bufio"
	"fmt"
	gpbackupUtils "github.com/greenplum-db/gpbackup/utils"
	"io"
	"os"
	"regexp"
)

type ConvertMaster struct {
	pgUpgradePath string
}

func NewConvertMaster(pgUpgradePath string) ConvertMaster {
	return ConvertMaster{pgUpgradePath: pgUpgradePath}
}

func (c ConvertMaster) GetStatus() (*pb.UpgradeStepStatus, error) {
	var masterUpgradeStatus *pb.UpgradeStepStatus
	pgUpgradePath := c.pgUpgradePath

	masterUpgradeStatusFailed := &pb.UpgradeStepStatus{
		Step:   pb.UpgradeSteps_MASTERUPGRADE,
		Status: pb.StepStatus_FAILED,
	}

	if _, err := utils.System.Stat(pgUpgradePath); utils.System.IsNotExist(err) {
		gpbackupUtils.GetLogger().Info("setting status to PENDING")
		masterUpgradeStatus = &pb.UpgradeStepStatus{
			Step:   pb.UpgradeSteps_MASTERUPGRADE,
			Status: pb.StepStatus_PENDING,
		}
		return masterUpgradeStatus, nil
	}

	inprogressFiles, err := utils.System.FilePathGlob(pgUpgradePath + "/*.inprogress")
	if err != nil {
		fmt.Println("err is: ", err)
		return masterUpgradeStatusFailed, err
	}

	if inprogressFiles != nil {
		gpbackupUtils.GetLogger().Info("setting status to RUNNING")
		masterUpgradeStatus = &pb.UpgradeStepStatus{
			Step:   pb.UpgradeSteps_MASTERUPGRADE,
			Status: pb.StepStatus_RUNNING,
		}
		return masterUpgradeStatus, nil
	}

	doneFiles, doneErr := utils.System.FilePathGlob(pgUpgradePath + "/*.done")
	if doneErr != nil {
		fmt.Println("err is: ", err)
		return masterUpgradeStatusFailed, doneErr
	}

	if doneFiles == nil {
		gpbackupUtils.GetLogger().Info("setting status to RUNNING")
		masterUpgradeStatus = &pb.UpgradeStepStatus{
			Step:   pb.UpgradeSteps_MASTERUPGRADE,
			Status: pb.StepStatus_RUNNING,
		}
		return masterUpgradeStatus, nil
	}

	if c.IsUpgradeComplete(doneFiles) {
		gpbackupUtils.GetLogger().Info("setting status to COMPLETE")
		masterUpgradeStatus = &pb.UpgradeStepStatus{
			Step:   pb.UpgradeSteps_MASTERUPGRADE,
			Status: pb.StepStatus_COMPLETE,
		}
		return masterUpgradeStatus, nil
	}

	gpbackupUtils.GetLogger().Info("setting status to RUNNING")
	masterUpgradeStatus = &pb.UpgradeStepStatus{
		Step:   pb.UpgradeSteps_MASTERUPGRADE,
		Status: pb.StepStatus_RUNNING,
	}
	return masterUpgradeStatus, nil

	// TODO: look into the done file and figure out if the message "upgrade complete is there"?
	// TODO: Do we need to consider checking if the process still exists?
	// TODO: Check if the pg_upgrade process is done?
	//
}

func (c ConvertMaster) IsUpgradeComplete(doneFiles []string) bool {
	/* Get the latest done file
	 * Parse and find the "upgrade complete" and return true.
	 * otherwise, return false.
	 */

	latestDoneFile := doneFiles[0]
	fi, err := utils.System.Stat(latestDoneFile)
	if err != nil {
		fmt.Println(err)
		gpbackupUtils.GetLogger().Error("IsUpgradeComplete: %v", err)
		return false
	}

	latestDoneFileModTime := fi.ModTime()
	for i := 1; i < len(doneFiles); i++ {
		doneFile := doneFiles[i]
		fi, err = os.Stat(doneFile)
		if err != nil {
			// TODO: What should we do here?
			continue
		}

		if fi.ModTime().After(latestDoneFileModTime) {
			latestDoneFile = doneFiles[i]
			latestDoneFileModTime = fi.ModTime()
		}
	}

	f, err := utils.System.Open(latestDoneFile)
	if err != nil {
		fmt.Println(err)
	}
	defer f.Close()
	r := bufio.NewReader(f)
	line, err := r.ReadString('\n')

	// TODO: Needs more error checking
	for err != io.EOF {
		if err != nil {
			gpbackupUtils.GetLogger().Error("IsUpgradeComplete: %v", err)
			return false
		}
		gpbackupUtils.GetLogger().Debug(line)
		re := regexp.MustCompile("Upgrade complete")
		if re.FindString(line) != "" {
			return true
		}

		line, err = r.ReadString('\n')
	}
	return false
}
