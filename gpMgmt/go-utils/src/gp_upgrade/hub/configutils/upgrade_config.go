package configutils

import (
	"errors"
)

type UpgradeConfig struct {
	oldClusterReader Reader
	newClusterReader Reader
}

func GetUpgradeConfig() (UpgradeConfig, error) {
	oldConfReader := Reader{}
	oldConfReader.OfOldClusterConfig()
	err := oldConfReader.Read()
	if err != nil {
		return UpgradeConfig{}, err
	}
	newConfReader := Reader{}
	newConfReader.OfNewClusterConfig()
	err = newConfReader.Read()
	if err != nil {
		return UpgradeConfig{}, err
	}
	return UpgradeConfig{oldClusterReader: oldConfReader, newClusterReader: newConfReader}, nil
}

func (u *UpgradeConfig) GetMasterPorts() (int, int, error) {
	masterDbID := 1 // We are assuming that the master dbid will always be 1

	oldMasterPort := u.oldClusterReader.GetPortForSegment(masterDbID)
	if oldMasterPort == -1 {
		return -1, -1, errors.New("could not find port from old config")
	}

	newMasterPort := u.newClusterReader.GetPortForSegment(masterDbID)
	if newMasterPort == -1 {
		return -1, -1, errors.New("could not find port from new config")
	}

	return oldMasterPort, newMasterPort, nil
}

func (u *UpgradeConfig) GetMasterDataDirs() (string, string, error) {

	oldMasterDataDir := u.oldClusterReader.GetMasterDataDir()
	if oldMasterDataDir == "" {
		return "", "", errors.New("could not find old master data directory")
	}

	newMasterDataDir := u.newClusterReader.GetMasterDataDir()
	if newMasterDataDir == "" {
		return "", "", errors.New("could not find new master data directory")
	}

	return oldMasterDataDir, newMasterDataDir, nil
}
