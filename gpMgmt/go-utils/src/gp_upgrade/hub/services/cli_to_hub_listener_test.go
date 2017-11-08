package services_test

import (
	"gp_upgrade/hub/services"
	"gp_upgrade/idl"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("hub", func() {
	Describe("creates a reply", func() {
		It("sends normal messages under good condition", func() {
			listener := services.NewCliToHubListener()
			var fakeStatusUpgradeRequest *idl.StatusUpgradeRequest
			formulatedResponse, err := listener.StatusUpgrade(nil, fakeStatusUpgradeRequest)
			Expect(err).To(BeNil())
			countOfStatuses := len(formulatedResponse.GetListOfUpgradeStepStatuses())
			Expect(countOfStatuses).ToNot(BeZero())
		})
	})
})
