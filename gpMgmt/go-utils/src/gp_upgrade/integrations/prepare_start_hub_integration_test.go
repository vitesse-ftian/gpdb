package integrations_test

import (
	"log"
	"os"
	"os/exec"
	"path"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gexec"
)

var _ = Describe("integration tests running on master only", func() {

	Describe("gp_upgrade prepare", func() {
		Describe("start-hub", func() {
			basicHappyPathCheck := func() {
				killHub()
				gpUpgradeSession := runCommand("prepare", "start-hub")
				Eventually(gpUpgradeSession).Should(Exit(0))

				verificationCmd := exec.Command("bash", "-c", `ps -ef | grep -q "[g]p_upgrade_hub"`)
				verificationSession, err := Start(verificationCmd, GinkgoWriter, GinkgoWriter)
				Expect(err).NotTo(HaveOccurred())
				Eventually(verificationSession).Should(Exit(0))
			}

			It("finds the right hub binary and starts a daemonized process", basicHappyPathCheck)

			It("works even if run from the same directory as where the binaries are", func() {
				// because we don't want the grep to shell expand
				hubDirectoryPath := path.Dir(hubBinaryPath)
				previousDirectory, err := os.Getwd()
				if err != nil {
					log.Fatal(err)
				}
				defer os.Chdir(previousDirectory)

				err = os.Chdir(hubDirectoryPath)
				if err != nil {
					log.Fatal(err)
				}

				basicHappyPathCheck()
			})

			It("returns error if gp_upgrade_hub is already running", func() {
				//start a hub if necessary
				ensureHubIsUp()

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
