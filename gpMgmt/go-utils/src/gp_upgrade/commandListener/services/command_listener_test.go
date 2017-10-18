package services_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"context"
	"github.com/pkg/errors"
	"gp_upgrade/commandListener/services"
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"
	"os"
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
	Describe("checking disk space", func() {
		It("returns information that a shell call got about filesystems", func() {
			listener := services.NewCommandListener()
			var df_output = `Filesystem   Attribute1   ColumnB  AFieldNamedAvail
				/nice/name/mount/store/volume  100Gi  10Gi 10% /data`
			utils.System.ExecCmdOutput = func(name string, args ...string) ([]byte, error) {
				return []byte(df_output), nil
			}
			resp, err := listener.CheckDiskUsage(nil, &pb.CheckDiskUsageRequest{})
			Expect(err).To(BeNil())
			Expect(resp.FilesystemUsageList).To(Equal(df_output))
		})
	})

	Describe("checking disk space", func() {
		It("returns an error if shell calls about filesystems fails", func() {
			utils.System.ExecCmdOutput = func(name string, args ...string) ([]byte, error) {
				return nil, os.ErrNotExist
			}
			listener := services.NewCommandListener()
			_, err := listener.CheckDiskUsage(nil, &pb.CheckDiskUsageRequest{})
			Expect(err).To(HaveOccurred())
		})
	})
})
