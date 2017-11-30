package integrations_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	. "github.com/onsi/gomega/gexec"
)

// has one test needing hub up; one test needing it down
// ultimately, the status command isn't uniquely responsible for the cases where the hub is down
// consider moving this file's unhappy path alongside the `prepare start-hub` integration tests
var _ = Describe("status", func() {
	BeforeEach(ensureHubIsUp)
	AfterEach(killHub)

	Describe("upgrade", func() {
		It("Reports some demo status from the hub", func() {
			statusSession := runCommand("status", "upgrade")
			Eventually(statusSession).Should(Exit(0))

			expectedDemoOutputPart1 := `PENDING - Configuration Check`
			expectedDemoOutputPart2 := `PENDING - Install binaries on segments`
			Eventually(statusSession).Should(gbytes.Say(expectedDemoOutputPart1))
			Eventually(statusSession).Should(gbytes.Say(expectedDemoOutputPart2))
		})

		It("Explodes if the hub isn't up", func() {
			//beforeSuite + individual tests' afterEach all stop the hub

			killHub()
			statusSession := runCommand("status", "upgrade")
			expectedErrorOutput := `ERROR - Unable to connect to hub`
			Eventually(statusSession.Err).Should(gbytes.Say(expectedErrorOutput))
			Eventually(statusSession).Should(Exit(1))
		})
	})
})
