package services_test

import (
	"gp_upgrade/hub/logger"
	"gp_upgrade/hub/services"
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"

	"errors"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"os"
)

var _ = Describe("object count tests", func() {
	var (
		listener                    pb.CliToHubServer
		fakeShutdownClustersRequest *pb.PrepareShutdownClustersRequest
		fakeLogger                  logger.LogEntry
	)

	AfterEach(func() {
		utils.System = utils.InitializeSystemFunctions()
	})

	Describe("PrepareShutdownClusters", func() {
		Describe("ignoring the go routine", func() {
			initialSetup := func() (pb.CliToHubServer, *pb.PrepareShutdownClustersRequest, logger.LogEntry) {
				// If the channel doesn't have enough capacity, it will block.
				// This will give a really vauge error message during testing.
				// Make the channel buffer LARGE!
				muchLargerThanNeeded := 999
				infoChannel := make(chan string, muchLargerThanNeeded)
				fakeLogger := logger.LogEntry{Info: infoChannel,
					Error: make(chan string, muchLargerThanNeeded), Done: make(chan bool, muchLargerThanNeeded)}
				listener := services.NewCliToHubListener(fakeLogger, &fakeStubClusterPair{})

				fakeShutdownClustersRequest := &pb.PrepareShutdownClustersRequest{OldBinDir: "/old/path/bin",
					NewBinDir: "/new/path/bin"}

				return listener, fakeShutdownClustersRequest, fakeLogger
			}

			BeforeEach(func() {
				listener, fakeShutdownClustersRequest, fakeLogger = initialSetup()
			})

			It("returns successfully", func() {
				utils.System.Getenv = func(s string) string { return "foo" }
				utils.System.RemoveAll = func(s string) error { return nil }
				utils.System.MkdirAll = func(s string, perm os.FileMode) error { return nil }

				_, err := listener.PrepareShutdownClusters(nil, fakeShutdownClustersRequest)
				Expect(err).To(BeNil())
				Eventually(fakeLogger.Info).Should(Receive(Equal("starting PrepareShutdownClusters()")))
			})

			It("fails if home directory not available in environment", func() {
				utils.System.Getenv = func(s string) string { return "" }

				_, err := listener.PrepareShutdownClusters(nil, fakeShutdownClustersRequest)
				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(ContainSubstring("home directory environment variable"))
				Eventually(fakeLogger.Info).Should(Receive(Equal("starting PrepareShutdownClusters()")))
			})

			It("fails if the cluster configuration setup can't be loaded", func() {
				utils.System.Getenv = func(s string) string { return "foo" }
				utils.System.RemoveAll = func(s string) error { return nil }
				utils.System.MkdirAll = func(s string, perm os.FileMode) error { return nil }

				failingListener := services.NewCliToHubListener(fakeLogger, &fakeFailingClusterPair{})

				_, err := failingListener.PrepareShutdownClusters(nil, fakeShutdownClustersRequest)
				Expect(err).ToNot(BeNil())
				Expect(err.Error()).To(ContainSubstring("boom"))
				Eventually(fakeLogger.Info).Should(Receive(Equal("starting PrepareShutdownClusters()")))
			})
		})
	})
})

type fakeStubClusterPair struct{}

func (c *fakeStubClusterPair) StopEverything(str string, entry *logger.LogEntry) {}
func (c *fakeStubClusterPair) Init(oldPath string, newPath string) error         { return nil }

type fakeFailingClusterPair struct{}

func (c *fakeFailingClusterPair) StopEverything(str string, entry *logger.LogEntry) {}
func (c *fakeFailingClusterPair) Init(oldPath string, newPath string) error {
	return errors.New("boom")
}
