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
)

var _ = Describe("preparer", func() {

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

	Describe("VerifyConnectivity", func() {
		It("returns nil when hub answers PingRequest", func() {
			testutils.SetupTestLogger()

			client.EXPECT().Ping(
				gomock.Any(),
				&pb.PingRequest{},
			).Return(&pb.PingReply{}, nil)

			preparer := commanders.Preparer{}
			err := preparer.VerifyConnectivity(client)
			Expect(err).To(BeNil())
		})

		It("returns err when hub doesn't answer PingRequest", func() {
			testutils.SetupTestLogger()
			commanders.NumberOfConnectionAttempt = 1

			client.EXPECT().Ping(
				gomock.Any(),
				&pb.PingRequest{},
			).Return(&pb.PingReply{}, errors.New("not answering ping")).Times(commanders.NumberOfConnectionAttempt + 1)

			preparer := commanders.Preparer{}
			err := preparer.VerifyConnectivity(client)
			Expect(err).ToNot(BeNil())
		})
		It("returns success if Ping eventually answers", func() {
			testutils.SetupTestLogger()

			client.EXPECT().Ping(
				gomock.Any(),
				&pb.PingRequest{},
			).Return(&pb.PingReply{}, errors.New("not answering ping"))

			client.EXPECT().Ping(
				gomock.Any(),
				&pb.PingRequest{},
			).Return(&pb.PingReply{}, nil)

			preparer := commanders.Preparer{}
			err := preparer.VerifyConnectivity(client)
			Expect(err).To(BeNil())
		})
	})

})
