package integrations_test

import (
	"gp_upgrade/testUtils"
	"io/ioutil"
	"os"

	"gp_upgrade/hub/configutils"

	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

// needs the cli and the hub
var _ = Describe("check", func() {

	var (
		save_home_dir string
	)

	BeforeEach(func() {
		save_home_dir = testUtils.ResetTempHomeDir()
		/* We need to make sure that we're starting up a new hub. This ensures that we're running a hub with a specific HOME directory.
		 * We can also consider changing the test such that we are removing the new_cluster_config file that we generate per run.
		 */
		restartHub()
	})

	AfterEach(func() {
		os.Setenv("HOME", save_home_dir)
		killHub()
	})

	Describe("when a greenplum master db on localhost is up and running", func() {
		It("happy: the database configuration is saved to a specified location", func() {
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

			// should there be something checking the version file being laid down as well?
		})
	})
})
