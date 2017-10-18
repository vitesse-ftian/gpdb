package commands

import (
	"github.com/golang/mock/gomock"
	"github.com/greenplum-db/gpbackup/testutils"
	dpm "github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	pb "gp_upgrade/idl"
	mockpb "gp_upgrade/mock_idl"
	"gp_upgrade/shellParsers"
	"gp_upgrade/testUtils"
	"os"
	"testing"
)

var _ = Describe("disk_usage test", func() {
	var (
		subject     Hub
		saveHomeDir string
		shellParser *shellParsers.RealShellParser
		client      *mockpb.MockCommandListenerClient
		t           *testing.T
		ctrl        *gomock.Controller
		testLogger  *dpm.Logger
		testStdout  *gbytes.Buffer
		testStderr  *gbytes.Buffer
		testLogfile *gbytes.Buffer
	)

	BeforeEach(func() {
		testLogger, testStdout, testStderr, testLogfile = testutils.SetupTestLogger()
		saveHomeDir = testUtils.ResetTempHomeDir()
		testUtils.WriteSampleConfig()

		shellParser = &shellParsers.RealShellParser{}

		ctrl = gomock.NewController(t)
		client = mockpb.NewMockCommandListenerClient(ctrl)
		subject = Hub{}
	})

	AfterEach(func() {
		os.Setenv("HOME", saveHomeDir)
		defer ctrl.Finish()
	})

	Describe("check disk usage", func() {
		Describe("happy", func() {

			It("prints that disk usage check passed", func() {
				var clients []pb.CommandListenerClient
				client.EXPECT().CheckDiskUsage(
					gomock.Any(),
					&pb.CheckDiskUsageRequest{},
				).Return(&pb.CheckDiskUsageReply{FilesystemUsageList: "eventually something to be parsed"}, nil)
				clients = append(clients, client)
				buffer := gbytes.NewBuffer()
				subject.CheckDiskUsage(clients, buffer)

				Expect(string(buffer.Contents())).ToNot(ContainSubstring(`Could not get disk usage from: `))
				Expect(string(buffer.Contents())).To(ContainSubstring(`gp_upgrade: Disk Usage Check [OK]`))
			})
		})
	})
})
