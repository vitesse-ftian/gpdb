package commanders_test

import (
	"errors"
	"gp_upgrade/cli/commanders"
	pb "gp_upgrade/idl"
	mockpb "gp_upgrade/mock_idl"

	"github.com/golang/mock/gomock"
	"github.com/greenplum-db/gpbackup/testutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("reporter", func() {

	var (
		client *mockpb.MockCliToHubClient
		ctrl   *gomock.Controller
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		client = mockpb.NewMockCliToHubClient(ctrl)
	})

	AfterEach(func() {
		defer ctrl.Finish()
	})

	Describe("OverallUpgradeStatus", func() {
		It("prints out status when hub rpc returns something normal", func() {
			//testLogger, testStdout, testStderr, testLogfile := testutils.SetupTestLogger()
			_, testStdout, _, _ := testutils.SetupTestLogger()
			fakeCheckStepStatus := &pb.UpgradeStepStatus{
				Step:   pb.UpgradeSteps_CHECK_CONFIG,
				Status: pb.StepStatus_RUNNING,
			}
			fakeSegInstallStepStatus := &pb.UpgradeStepStatus{
				Step:   pb.UpgradeSteps_SEGINSTALL,
				Status: pb.StepStatus_PENDING,
			}
			fakeStatusUpgradeReply := &pb.StatusUpgradeReply{}
			fakeStatusUpgradeReply.ListOfUpgradeStepStatuses =
				append(fakeStatusUpgradeReply.ListOfUpgradeStepStatuses,
					fakeCheckStepStatus, fakeSegInstallStepStatus)

			client.EXPECT().StatusUpgrade(
				gomock.Any(),
				&pb.StatusUpgradeRequest{},
			).Return(fakeStatusUpgradeReply, nil)

			reporter := commanders.NewReporter(client)
			err := reporter.OverallUpgradeStatus()
			Expect(err).To(BeNil())
			Eventually(testStdout).Should(gbytes.Say("RUNNING - Configuration Check"))
			Eventually(testStdout).Should(gbytes.Say("PENDING - Install binaries on segments"))
		})

		It("prints out an error when connection cannot be established to the hub", func() {
			_, _, testStderr, _ := testutils.SetupTestLogger()
			client.EXPECT().StatusUpgrade(
				gomock.Any(),
				&pb.StatusUpgradeRequest{},
			).Return(nil, errors.New("Force failure connection"))

			reporter := commanders.NewReporter(client)
			err := reporter.OverallUpgradeStatus()
			Expect(err).ToNot(BeNil())
			Eventually(testStderr).Should(gbytes.Say("ERROR - Unable to connect to hub"))

		})
	})

})
