package commanders_test

import (
	"errors"
	"gp_upgrade/cli/commanders"
	pb "gp_upgrade/idl"
	mockpb "gp_upgrade/mock_idl"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/greenplum-db/gpbackup/testutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("object count tests", func() {

	var (
		client *mockpb.MockCliToHubClient
		t      *testing.T
		ctrl   *gomock.Controller
	)

	BeforeEach(func() {
		ctrl = gomock.NewController(t)
		client = mockpb.NewMockCliToHubClient(ctrl)
	})

	AfterEach(func() {
		defer ctrl.Finish()
	})
	Describe("Execute", func() {
		It("prints out that check object count request was processed", func() {
			//testLogger, testStdout, testStderr, testLogfile := testutils.SetupTestLogger()
			_, testStdout, _, _ := testutils.SetupTestLogger()

			fakeCountArray := []*pb.CountPerDb{}
			fakeCountTemplate1 := &pb.CountPerDb{DbName: "template1", AoCount: 1, HeapCount: 2}
			fakeCountArray = append(fakeCountArray, fakeCountTemplate1)
			fakeCountPostgres := &pb.CountPerDb{DbName: "postgres", AoCount: 2, HeapCount: 3}
			fakeCountArray = append(fakeCountArray, fakeCountPostgres)
			fakeCheckObjectCountReply := &pb.CheckObjectCountReply{ListOfCounts: fakeCountArray}

			client.EXPECT().CheckObjectCount(
				gomock.Any(),
				&pb.CheckObjectCountRequest{DbPort: 9999},
			).Return(fakeCheckObjectCountReply, nil)

			request := commanders.NewObjectCountChecker(client)
			err := request.Execute(9999)
			Expect(err).To(BeNil())
			Eventually(testStdout).Should(gbytes.Say("Checking object counts in database: template1"))
			Eventually(testStdout).Should(gbytes.Say("Number of AO objects - 1"))
			Eventually(testStdout).Should(gbytes.Say("Number of heap objects - 2"))
			Eventually(testStdout).Should(gbytes.Say("Checking object counts in database: postgres"))
			Eventually(testStdout).Should(gbytes.Say("Number of AO objects - 2"))
			Eventually(testStdout).Should(gbytes.Say("Number of heap objects - 3"))
			Eventually(testStdout).Should(gbytes.Say("Check object count request is processed."))
		})

		It("prints out an error when connection cannot be established to the hub", func() {
			_, _, testStderr, _ := testutils.SetupTestLogger()
			client.EXPECT().CheckObjectCount(
				gomock.Any(),
				&pb.CheckObjectCountRequest{DbPort: 9999},
			).Return(nil, errors.New("Force failure connection"))

			request := commanders.NewObjectCountChecker(client)
			err := request.Execute(9999)
			Expect(err).ToNot(BeNil())
			Eventually(testStderr).Should(gbytes.Say("ERROR - Unable to connect to hub"))

		})
	})
})
