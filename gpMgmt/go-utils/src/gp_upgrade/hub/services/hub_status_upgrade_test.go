package services_test

import (
	"errors"
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"
	"os"

	"github.com/greenplum-db/gpbackup/testutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gp_upgrade/hub/services"
)

var _ = Describe("hub", func() {
	BeforeEach(func() {
		testutils.SetupTestLogger() // extend to capture the values in a var if future tests need it
	})
	Describe("creates a reply", func() {
		It("sends status messages under good condition", func() {
			listener := services.NewCliToHubListener()
			var fakeStatusUpgradeRequest *pb.StatusUpgradeRequest
			formulatedResponse, err := listener.StatusUpgrade(nil, fakeStatusUpgradeRequest)
			Expect(err).To(BeNil())
			countOfStatuses := len(formulatedResponse.GetListOfUpgradeStepStatuses())
			Expect(countOfStatuses).ToNot(BeZero())
		})
	})
	Describe("Status of PrepareNewClusterConfig", func() {
		AfterEach(func() {
			//any mocking of utils.System function pointers should be reset by calling InitializeSystemFunctions
			utils.System = utils.InitializeSystemFunctions()
		})

		It("marks this step pending if there's no new cluster config file", func() {
			utils.System.Stat = func(filename string) (os.FileInfo, error) {
				return nil, errors.New("Cannot find file") /* This is normally a PathError */
			}
			stepStatus, err := services.GetPrepareNewClusterConfigStatus()
			Expect(err).To(BeNil()) // convert file-not-found errors into stepStatus
			Expect(stepStatus.Step).To(Equal(pb.UpgradeSteps_PREPARE_INIT_CLUSTER))
			Expect(stepStatus.Status).To(Equal(pb.StepStatus_PENDING))
		})
		It("marks this step complete if there is a new cluster config file", func() {
			utils.System.Stat = func(filename string) (os.FileInfo, error) {
				return nil, nil
			}

			stepStatus, err := services.GetPrepareNewClusterConfigStatus()
			Expect(err).To(BeNil())
			Expect(stepStatus.Step).To(Equal(pb.UpgradeSteps_PREPARE_INIT_CLUSTER))
			Expect(stepStatus.Status).To(Equal(pb.StepStatus_COMPLETE))

		})

	})
})
