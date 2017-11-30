package integrations_test

import (
	"fmt"
	"gp_upgrade/hub/configutils"
	"gp_upgrade/testUtils"
	"io/ioutil"
	"os"

	"github.com/onsi/gomega/gbytes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

// needs the cli and the hub
// the `prepare start-hub` tests are currently in master_only_integration_test
var _ = Describe("prepare", func() {
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

	/* This is demonstrating the limited implmentation of init-cluster.
	    Assuming the user has already set up their new cluster, they should `init-cluster`
		with the port at which they stood it up, so the upgrade tool can create new_cluster_config

		In the future, the upgrade tool might take responsibility for starting its own cluster,
		in which case it won't need the port, but would still generate new_cluster_config
	*/
	Describe("Given that a gpdb cluster is up, in this case reusing the single cluster for other test.", func() {
		It("can save the database configuration json under the name 'new cluster'", func() {
			statusSessionPending := runCommand("status", "upgrade")
			Eventually(statusSessionPending).Should(gbytes.Say("PENDING - Initialize upgrade target cluster"))

			port := os.Getenv("PGPORT")
			session := runCommand("prepare", "init-cluster", "--port", port, "&")

			if session.ExitCode() != 0 {
				fmt.Println("make sure greenplum is running")
			}
			Eventually(session).Should(Exit(0))

			statusSession := runCommand("status", "upgrade")
			Eventually(statusSession).Should(gbytes.Say("COMPLETE - Initialize upgrade target cluster"))

			// check file
			_, err := ioutil.ReadFile(configutils.GetNewClusterConfigFilePath())
			testUtils.Check("cannot read file", err)

			reader := configutils.NewReader()
			reader.OfNewClusterConfig()
			err = reader.Read()
			testUtils.Check("cannot read config", err)

			// for extra credit, read db and compare info
			Expect(len(reader.GetSegmentConfiguration())).To(BeNumerically(">", 1))
		})
	})
})
