package commands

import (
	"github.com/golang/mock/gomock"
	"github.com/greenplum-db/gpbackup/testutils"
	dpm "github.com/greenplum-db/gpbackup/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"gp_upgrade/hub/configutils"
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
		client1     *mockpb.MockCommandListenerClient
		client2     *mockpb.MockCommandListenerClient
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
		client1 = mockpb.NewMockCommandListenerClient(ctrl)
		client2 = mockpb.NewMockCommandListenerClient(ctrl)
		subject = Hub{}
	})

	AfterEach(func() {
		os.Setenv("HOME", saveHomeDir)
		defer ctrl.Finish()
	})

	Describe("check disk usage", func() {
		Describe("all filesystems on all hosts have enough space", func() {
			It("prints that disk usage check passed", func() {
				var expectedFilesystemsUsageHost1 []*pb.FileSysUsage
				expectedFilesystemsUsageHost1 = append(expectedFilesystemsUsageHost1, &pb.FileSysUsage{Filesystem: "first filesystem", Usage: 25.4})
				expectedFilesystemsUsageHost1 = append(expectedFilesystemsUsageHost1, &pb.FileSysUsage{Filesystem: "/second/filesystem", Usage: 24.2})
				var expectedFilesystemsUsageHost2 []*pb.FileSysUsage
				expectedFilesystemsUsageHost2 = append(expectedFilesystemsUsageHost1, &pb.FileSysUsage{Filesystem: "slightly different filesystem", Usage: 26.4})
				expectedFilesystemsUsageHost2 = append(expectedFilesystemsUsageHost1, &pb.FileSysUsage{Filesystem: "/second/filesystem2", Usage: 31.4})

				var clients []configutils.ClientAndHostname
				client1.EXPECT().CheckDiskUsage(
					gomock.Any(),
					&pb.CheckDiskUsageRequest{},
				).Return(&pb.CheckDiskUsageReply{ListOfFileSysUsage: expectedFilesystemsUsageHost1}, nil)
				client2.EXPECT().CheckDiskUsage(
					gomock.Any(),
					&pb.CheckDiskUsageRequest{},
				).Return(&pb.CheckDiskUsageReply{ListOfFileSysUsage: expectedFilesystemsUsageHost2}, nil)

				hostname1 := "aspenwood"
				hostname2 := "briar"

				clients = append(clients,
					configutils.ClientAndHostname{Client: client1, Hostname: hostname1},
					configutils.ClientAndHostname{Client: client2, Hostname: hostname2})
				buffer := gbytes.NewBuffer()
				subject.CheckDiskUsage(clients, buffer)

				Expect(string(buffer.Contents())).ToNot(ContainSubstring(`Could not get disk usage from: `))
				Expect(string(buffer.Contents())).ToNot(ContainSubstring(`WARNING`))
				Expect(string(buffer.Contents())).To(ContainSubstring(`gp_upgrade: Disk Usage Check [OK]`))
			})
		})

		Describe("one filesystem on one host is too full", func() {
			It("prints a warning for the full filesystem but otherwise shows success", func() {
				var expectedFilesystemsUsageHost1 []*pb.FileSysUsage
				expectedFilesystemsUsageHost1 = append(expectedFilesystemsUsageHost1, &pb.FileSysUsage{Filesystem: "first filesystem", Usage: 25.4})
				expectedFilesystemsUsageHost1 = append(expectedFilesystemsUsageHost1, &pb.FileSysUsage{Filesystem: "/second/filesystem", Usage: 98.6})
				var expectedFilesystemsUsageHost2 []*pb.FileSysUsage
				expectedFilesystemsUsageHost2 = append(expectedFilesystemsUsageHost1, &pb.FileSysUsage{Filesystem: "slightly different filesystem", Usage: 26.4})
				expectedFilesystemsUsageHost2 = append(expectedFilesystemsUsageHost1, &pb.FileSysUsage{Filesystem: "/second/filesystem2", Usage: 31.4})

				var clients []configutils.ClientAndHostname
				client1.EXPECT().CheckDiskUsage(
					gomock.Any(),
					&pb.CheckDiskUsageRequest{},
				).Return(&pb.CheckDiskUsageReply{ListOfFileSysUsage: expectedFilesystemsUsageHost1}, nil)
				client2.EXPECT().CheckDiskUsage(
					gomock.Any(),
					&pb.CheckDiskUsageRequest{},
				).Return(&pb.CheckDiskUsageReply{ListOfFileSysUsage: expectedFilesystemsUsageHost2}, nil)

				hostname1 := "aspenwood"
				hostname2 := "briar"

				clients = append(clients,
					configutils.ClientAndHostname{Client: client1, Hostname: hostname1},
					configutils.ClientAndHostname{Client: client2, Hostname: hostname2})
				buffer := gbytes.NewBuffer()
				subject.CheckDiskUsage(clients, buffer)

				Expect(string(buffer.Contents())).To(ContainSubstring(`diskspace check - aspenwood - WARNING /second/filesystem 99% use`))
				Expect(string(buffer.Contents())).ToNot(ContainSubstring(`gp_upgrade: Disk Usage Check [OK]`))

			})
		})
	})
})
