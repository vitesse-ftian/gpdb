package services

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"context"
	"github.com/pkg/errors"
	pb "gp_upgrade/idl"
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
			listener := NewCommandListener()
			resp, err := listener.CheckUpgradeStatus(context.TODO(), nil)
			Expect(resp.ProcessList).To(Equal("shell command output"))
			Expect(err).To(BeNil())
		})

		It("returns only err if anything is reported as an error", func() {
			utils.System.ExecCmdOutput = func(name string, args ...string) ([]byte, error) {
				return []byte("stdout during error"), errors.New("couldn't find bash")
			}
			listener := NewCommandListener()
			resp, err := listener.CheckUpgradeStatus(context.TODO(), nil)
			Expect(resp).To(BeNil())
			Expect(err.Error()).To(Equal("couldn't find bash"))
		})
	})
	Describe("checking disk space", func() {
		It("returns information that a helper function got about filesystems", func() {
			getDiskUsage := func() (map[string]float64, error) {
				fakeDiskUsage := make(map[string]float64)
				fakeDiskUsage["/data"] = 25.4
				return fakeDiskUsage, nil
			}
			listener := &commandListenerImpl{getDiskUsage}

			resp, err := listener.CheckDiskUsageOnAgents(nil, &pb.CheckDiskUsageRequestToAgent{})
			Expect(err).To(BeNil())
			for _, val := range resp.ListOfFileSysUsage {
				if val.Filesystem == "/data" {
					Expect(val.Usage).To(BeNumerically("~", 25.4, 0.001))
					break

				}
			}
		})

		It("returns an error if the helper function fails", func() {
			getDiskUsage := func() (map[string]float64, error) {
				return nil, errors.New("fake error")
			}
			listener := &commandListenerImpl{getDiskUsage}
			_, err := listener.CheckDiskUsageOnAgents(nil, &pb.CheckDiskUsageRequestToAgent{})
			Expect(err).To(HaveOccurred())
		})
	})
})
