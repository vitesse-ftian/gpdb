package integrations_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
	"os"
	"os/exec"
)

var _ = Describe("integration tests running on master only", func() {

	Describe("gp_upgrade prepare", func() {
		Describe("start-hub", func() {
			AfterEach(func() {
				pkillCmd := exec.Command("pkill", "gp_upgrade_hub")
				pkillCmd.Run()
			})

			It("finds the right hub binary and starts a daemonized process", func() {
				gpUpgradeSession := runCommand("prepare", "start-hub")
				Eventually(gpUpgradeSession).Should(Exit(0))

				verificationCmd := exec.Command("bash", "-c", "ps -ef | grep -q gp_upgrade_hub")
				verificationSession, err := Start(verificationCmd, GinkgoWriter, GinkgoWriter)
				Expect(err).NotTo(HaveOccurred())
				Eventually(verificationSession).Should(Exit(0))
			})

			It("returns error if gp_upgrade_hub is already running", func() {
				//start a hub if necessary
				runCommand("prepare", "start-hub")

				//second start should return error
				secondSession := runCommand("prepare", "start-hub")
				Eventually(secondSession).Should(Exit(1))
			})

			It("errs out if doesn't find gp_upgrade_hub on the path", func() {
				origPath := os.Getenv("PATH")
				os.Setenv("PATH", "")
				gpUpgradeSession := runCommand("prepare", "start-hub")
				Eventually(gpUpgradeSession).ShouldNot(Exit(0))
				os.Setenv("PATH", origPath)
			})
		})
	})
})
