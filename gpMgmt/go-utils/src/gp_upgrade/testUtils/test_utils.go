package testUtils

import (
	"fmt"
	"gp_upgrade/config"
	"io/ioutil"
	"os"

	"path"
)

const (
	TempHomeDir = "/tmp/gp_upgrade_test_temp_home_dir"

	SAMPLE_JSON = `[{
    "address": "briarwood",
    "content": 2,
    "dbid": 7,
    "fsedbid": 7,
    "fsefsoid": "3052",
    "fselocation": "/Users/pivotal/workspace/gpdb/gpAux/gpdemo/datadirs/dbfast_mirror3/demoDataDir2",
    "hostname": "briarwood",
    "mode": "s",
    "port": 25437,
    "preferred_role": "m",
    "replication_port": 25443,
    "role": "m",
    "san_mounts": null,
    "status": "u"
  },
  {
    "address": "aspen",
    "content": 1,
    "dbid": 6,
    "fsedbid": 6,
    "fsefsoid": "3052",
    "fselocation": "/Users/pivotal/workspace/gpdb/gpAux/gpdemo/datadirs/dbfast_mirror2/demoDataDir1",
    "hostname": "aspen.pivotal",
    "mode": "s",
    "port": 25436,
    "preferred_role": "m",
    "replication_port": 25442,
    "role": "m",
    "status": "u"
  }]`
)

func Check(msg string, e error) {
	if e != nil {
		panic(fmt.Sprintf("%s: %s\n", msg, e.Error()))
	}
}

func ResetTempHomeDir() string {
	configDir := path.Join(TempHomeDir, ".gp_upgrade")
	if _, err := os.Stat(configDir); !os.IsNotExist(err) {
		err = os.Chmod(configDir, 0700)
		Check("cannot change mod", err)
	}
	err := os.RemoveAll(TempHomeDir)
	Check("cannot remove temp home", err)
	save := os.Getenv("HOME")
	err = os.MkdirAll(TempHomeDir, 0700)
	Check("cannot create home temp dir", err)
	err = os.Setenv("HOME", TempHomeDir)
	Check("cannot set home dir", err)
	return save
}

func WriteSampleConfig() {
	err := os.MkdirAll(config.GetConfigDir(), 0700)
	Check("cannot create sample dir", err)
	err = ioutil.WriteFile(config.GetConfigFilePath(), []byte(SAMPLE_JSON), 0600)
	Check("cannot write sample config", err)
}
