package integrations_test

import (
	"os/exec"

	"gp_upgrade/testUtils"
	"io/ioutil"
	"os"

	"gp_upgrade/hub/configutils"

	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

var _ = Describe("check", func() {

	var (
		save_home_dir string
	)

	killHub := func() {
		//pkill gp_upgrade_ will kill both gp_upgrade_hub and gp_upgrade_agent
		pkillCmd := exec.Command("pkill", "gp_upgrade_")
		pkillCmd.Run()
	}

	BeforeEach(func() {
		save_home_dir = testUtils.ResetTempHomeDir()
	})

	AfterEach(func() {
		os.Setenv("HOME", save_home_dir)
		killHub()
	})

	Describe("when a greenplum master db on localhost is up and running", func() {
		It("happy: the database configuration is saved to a specified location", func() {

			prepareSession := runCommand("prepare", "start-hub")
			Eventually(prepareSession).Should(Exit(0))

			session := runCommand("check", "config", "--master-host", "localhost")

			if session.ExitCode() != 0 {
				fmt.Println("make sure greenplum is running")
			}
			Eventually(session).Should(Exit(0))
			// check file

			_, err := ioutil.ReadFile(configutils.GetConfigFilePath())
			testUtils.Check("cannot read file", err)

			reader := configutils.Reader{}
			reader.OfOldClusterConfig()
			err = reader.Read()
			testUtils.Check("cannot read config", err)

			// for extra credit, read db and compare info
			Expect(len(reader.GetSegmentConfiguration())).To(BeNumerically(">", 1))
		})
	})
})
