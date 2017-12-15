package cluster

import (
	"gp_upgrade/hub/configutils"
	"gp_upgrade/utils"

	"fmt"
	"gp_upgrade/hub/logger"
	"os"
	"os/exec"
	"path"
)

type PairOperator interface {
	Init(string, string) error
	StopEverything(string, *logger.LogEntry)
}

type Pair struct {
	upgradeConfig          configutils.UpgradeConfig
	oldMasterPort          int
	newMasterPort          int
	oldMasterDataDirectory string
	newMasterDataDirectory string
	oldBinDir              string
	newBinDir              string
}

func (cp *Pair) Init(oldBinDir string, newBinDir string) error {
	var err error
	cp.oldBinDir = oldBinDir
	cp.newBinDir = newBinDir

	cp.upgradeConfig, err = configutils.GetUpgradeConfig()
	if err != nil {
		return fmt.Errorf("couldn't read config files: %v", err)
	}

	cp.oldMasterPort, cp.newMasterPort, err = cp.upgradeConfig.GetMasterPorts()
	if err != nil {
		return err
	}

	cp.oldMasterDataDirectory, cp.newMasterDataDirectory, err = cp.upgradeConfig.GetMasterDataDirs()
	if err != nil {
		return err
	}

	return nil
}

func (cp *Pair) StopEverything(pathToGpstopStateDir string, logger *logger.LogEntry) {
	oldGpstopShellArgs := fmt.Sprintf("PGPORT=%d && MASTER_DATA_DIRECTORY=%s && %s/gpstop -a",
		cp.oldMasterPort, cp.oldMasterDataDirectory, cp.oldBinDir)
	runOldStopCmd := utils.System.ExecCommand("bash", "-c", oldGpstopShellArgs)

	newGpstopShellArgs := fmt.Sprintf("PGPORT=%d && MASTER_DATA_DIRECTORY=%s && %s/gpstop -a", cp.newMasterPort,
		cp.newMasterDataDirectory, cp.newBinDir)
	runNewStopCmd := utils.System.ExecCommand("bash", "-c", newGpstopShellArgs)

	stopCluster(runOldStopCmd, "gpstop.old", pathToGpstopStateDir, logger)
	stopCluster(runNewStopCmd, "gpstop.new", pathToGpstopStateDir, logger)
}

func stopCluster(stopCmd *exec.Cmd, baseName string, pathToGpstopStateDir string, logger *logger.LogEntry) {
	err := recordRunningState(pathToGpstopStateDir, baseName)
	if err != nil {
		logger.Error <- err.Error()
		return
	}

	err = stopCmd.Run()

	logger.Info <- fmt.Sprintf("finished stopping %s", baseName)

	if err != nil {
		logger.Error <- err.Error()
		recordFailedState(pathToGpstopStateDir, baseName, logger)
		return
	}
	recordCompleteState(pathToGpstopStateDir, baseName, logger)
}

func recordCompleteState(pathToGpstopStateDir string, baseName string, logger *logger.LogEntry) {
	_, err := utils.System.OpenFile(path.Join(pathToGpstopStateDir, fmt.Sprintf("%s.complete", baseName)), os.O_RDONLY|os.O_CREATE, 0700)
	if err != nil {
		logger.Error <- fmt.Sprintf("gpstop ran successfully, but couldn't create %s.complete file", baseName)
		logger.Error <- err.Error()
	}

	err = utils.System.Remove(path.Join(pathToGpstopStateDir, fmt.Sprintf("%s.running", baseName)))
	if err != nil {
		logger.Error <- fmt.Sprintf("gpstop ran successfully, but couldn't remove %s.running file", baseName)
		logger.Error <- err.Error()
	}
}

func recordRunningState(pathToGpstopStateDir string, baseName string) error {
	_, err := utils.System.OpenFile(path.Join(pathToGpstopStateDir, fmt.Sprintf("%s.running", baseName)), os.O_RDONLY|os.O_CREATE, 0700)
	return err
}

func recordFailedState(pathToGpstopStateDir string, baseName string, logger *logger.LogEntry) {
	_, err := utils.System.OpenFile(path.Join(pathToGpstopStateDir, fmt.Sprintf("%s.error", baseName)), os.O_RDONLY|os.O_CREATE, 0700)
	if err != nil {
		logger.Error <- err.Error()
	}

	err = utils.System.Remove(path.Join(pathToGpstopStateDir, fmt.Sprintf("%s.running", baseName)))
	if err != nil {
		logger.Error <- fmt.Sprintf("gpstop ran successfully, but couldn't remove %s.running file", baseName)
		logger.Error <- err.Error()
	}
}
