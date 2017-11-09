package services_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gp_upgrade/hub/services"
	"gp_upgrade/idl"
)

var _ = Describe("hub", func() {
	Describe("creates a reply", func() {
		It("sends status messages under good condition", func() {
			listener := services.NewCliToHubListener()
			var fakeStatusUpgradeRequest *idl.StatusUpgradeRequest
			formulatedResponse, err := listener.StatusUpgrade(nil, fakeStatusUpgradeRequest)
			Expect(err).To(BeNil())
			countOfStatuses := len(formulatedResponse.GetListOfUpgradeStepStatuses())
			Expect(countOfStatuses).ToNot(BeZero())
		})
	})
})
