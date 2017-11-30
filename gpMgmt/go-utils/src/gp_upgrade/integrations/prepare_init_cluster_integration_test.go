package integrations_test

import (
	"fmt"
	"gp_upgrade/hub/configutils"
	"gp_upgrade/testUtils"
	"io/ioutil"
	"os"
	"os/exec"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

var _ = Describe("prepare", func() {
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

	/* This is demonstrating the limited implmentation of init-cluster.
	    Assuming the user has already set up their new cluster, they should `init-cluster`
		with the port at which they stood it up, so the upgrade tool can create new_cluster_config

		In the future, the upgrade tool might take responsibility for starting its own cluster,
		in which case it won't need the port, but would still generate new_cluster_config
	*/
	Describe("Given that a gpdb cluster is up, in this case reusing the single cluster for other test.", func() {
		It("can save the database configuration json under the name 'new cluster'", func() {

			prepareSession := runCommand("prepare", "start-hub")
			Eventually(prepareSession).Should(Exit(0))

			port := os.Getenv("PGPORT")
			session := runCommand("prepare", "init-cluster", "--port", port)

			/* XXX: There will be a waiting game here once generating a cluster is implemented */

			if session.ExitCode() != 0 {
				fmt.Println("make sure greenplum is running")
			}
			Eventually(session).Should(Exit(0))
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
