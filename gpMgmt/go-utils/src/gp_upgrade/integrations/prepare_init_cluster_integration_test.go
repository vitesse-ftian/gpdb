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

// the `prepare start-hub` tests are currently in master_only_integration_test
var _ = Describe("prepare", func() {

	BeforeEach(func() {
		ensureHubIsUp()
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
