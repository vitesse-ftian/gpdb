package services_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"context"
	"github.com/pkg/errors"
	"gp_upgrade/commandListener/services"
	"gp_upgrade/utils"
)

var _ = Describe("CommandListener", func() {

	BeforeEach(func() {
	})

	AfterEach(func() {
		//any mocking of utils.System function pointers should be reset by calling InitializeSystemFunctions
		utils.InitializeSystemFunctions()
	})
	Describe("check upgrade status", func() {
		It("returns the shell command output", func() {
			utils.System.ExecCmdOutput = func(name string, args ...string) ([]byte, error) {
				return []byte("shell command output"), nil
			}
			listener := services.NewCommandListener()
			resp, err := listener.CheckUpgradeStatus(context.TODO(), nil)
			Expect(resp.ProcessList).To(Equal("shell command output"))
			Expect(err).To(BeNil())
		})

		It("returns only err if anything is reported as an error", func() {
			utils.System.ExecCmdOutput = func(name string, args ...string) ([]byte, error) {
				return []byte("stdout during error"), errors.New("couldn't find bash")
			}
			listener := services.NewCommandListener()
			resp, err := listener.CheckUpgradeStatus(context.TODO(), nil)
			Expect(resp).To(BeNil())
			Expect(err.Error()).To(Equal("couldn't find bash"))
		})
	})
})
