package integrations_test

import (
	"os/exec"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	. "github.com/onsi/gomega/gexec"
)

var _ = Describe("status", func() {
	killHub := func() {
		//pkill gp_upgrade_ will kill both gp_upgrade_hub and gp_upgrade_agent
		pkillCmd := exec.Command("pkill", "gp_upgrade_")
		pkillCmd.Run()
	}

	AfterEach(killHub)

	Describe("upgrade", func() {
		It("Reports some demo status from the hub", func() {
			prepareSession := runCommand("prepare", "start-hub")
			Eventually(prepareSession).Should(Exit(0))

			statusSession := runCommand("status", "upgrade")
			Eventually(statusSession).Should(Exit(0))

			expectedDemoOutputPart1 := `PENDING - Configuration Check`
			expectedDemoOutputPart2 := `PENDING - Install binaries on segments`
			Eventually(statusSession).Should(gbytes.Say(expectedDemoOutputPart1))
			Eventually(statusSession).Should(gbytes.Say(expectedDemoOutputPart2))
		})

		It("Explodes if the hub isn't up", func() {
			//beforeSuite + individual tests' afterEach all stop the hub

			statusSession := runCommand("status", "upgrade")
			expectedErrorOutput := `ERROR - Unable to connect to hub`
			Eventually(statusSession.Err).Should(gbytes.Say(expectedErrorOutput))
			Eventually(statusSession).Should(Exit(1))
		})
	})
})
