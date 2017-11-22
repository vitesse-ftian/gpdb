package commanders_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gp_upgrade/cli/commanders"
)

var _ = Describe("Version", func() {
	Describe("VersionString", func() {
		Context("when global var GpdbVersion is the empty string", func() {
			It("returns the default version", func() {
				commanders.GpdbVersion = ""
				Expect(commanders.VersionString()).To(Equal("gp_upgrade unknown version"))
			})
		})

		Context("when global var GpdbVersion is set to something", func() {
			It("returns what it's set to", func() {
				commanders.GpdbVersion = "Something"
				Expect(commanders.VersionString()).To(Equal("gp_upgrade version Something"))
			})
		})
	})
})
