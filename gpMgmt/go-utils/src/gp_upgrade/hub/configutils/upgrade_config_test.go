package configutils_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gp_upgrade/utils"

	"gp_upgrade/hub/configutils"
	"regexp"
	"strings"
)

const (
	MASTER_ONLY_JSON = `
[{
    "address": "briarwood",
    "content": -1,
    "dbid": 1,
    "fsedbid": 7,
    "fsefsoid": "3052",
    "fselocation": "/old/datadir",
    "hostname": "briarwood",
    "mode": "s",
    "port": 25437,
    "preferred_role": "m",
    "replication_port": 25443,
    "role": "m",
    "san_mounts": null,
    "status": "u"
  }]
`

	NEW_MASTER_JSON = `[{
    "address": "aspen",
    "content": -1,
    "dbid": 1,
    "fsedbid": 7,
    "fsefsoid": "3052",
    "fselocation": "/new/datadir",
    "hostname": "briarwood",
    "mode": "s",
    "port": 35437,
    "preferred_role": "m",
    "replication_port": 35443,
    "role": "m",
    "san_mounts": null,
    "status": "u"
  }]
`
)

var _ = Describe("ConfigutilsReader", func() {
	AfterEach(func() {
		utils.System = utils.InitializeSystemFunctions()
	})
	Describe("#UpgradeConfig", func() {
		Describe("reads a configuration for both clusters", func() {
			utils.System.ReadFile = func(filename string) ([]byte, error) {
				if strings.Contains(filename, "new_cluster_config.json") {
					return []byte(NEW_MASTER_JSON), nil
				} else if strings.Contains(filename, "cluster_config.json") {
					return []byte(MASTER_ONLY_JSON), nil
				}
				return nil, nil
			}
			upgradeConfig, err := configutils.GetUpgradeConfig()
			It("reads both configs properly", func() {
				Expect(err).To(BeNil())
			})
			It("gets the port properly", func() {
				oldPort, newPort, err := upgradeConfig.GetMasterPorts()
				Expect(err).To(BeNil())
				Expect(oldPort).To(Equal(25437))
				Expect(newPort).To(Equal(35437))
			})

			It("gets the datadirs propelry", func() {
				oldDataDir, newDataDir, err := upgradeConfig.GetMasterDataDirs()
				Expect(err).To(BeNil())
				Expect(oldDataDir).To(Equal("/old/datadir"))
				Expect(newDataDir).To(Equal("/new/datadir"))
			})

		})
		Describe("reads a config without master", func() {
			re := regexp.MustCompile(`"dbid": 1`)
			configWithoutMaster := re.ReplaceAllLiteralString(NEW_MASTER_JSON, `"dbid": 2`)
			re2 := regexp.MustCompile(`"content": -1`)
			configWithoutMaster = re2.ReplaceAllLiteralString(configWithoutMaster, `"content": 10`)

			utils.System.ReadFile = func(filename string) ([]byte, error) {
				if strings.Contains(filename, "new_cluster_config.json") {
					return []byte(configWithoutMaster), nil
				} else if strings.Contains(filename, "cluster_config.json") {
					return []byte(MASTER_ONLY_JSON), nil
				}
				return nil, nil
			}
			upgradeConfig, err := configutils.GetUpgradeConfig()
			It("reads both configs properly", func() {
				Expect(err).To(BeNil())
			})
			It("gets the port properly", func() {
				oldPort, newPort, err := upgradeConfig.GetMasterPorts()
				Expect(err).ToNot(BeNil())
				Expect(oldPort).To(Equal(-1))
				Expect(newPort).To(Equal(-1))
			})
			It("gets the datadirs propelry", func() {
				oldDataDir, newDataDir, err := upgradeConfig.GetMasterDataDirs()
				Expect(err).ToNot(BeNil())
				Expect(oldDataDir).To(Equal(""))
				Expect(newDataDir).To(Equal(""))
			})
		})
	})
})
