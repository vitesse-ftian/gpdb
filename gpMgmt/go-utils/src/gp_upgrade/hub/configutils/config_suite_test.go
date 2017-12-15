package configutils_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gp_upgrade/utils"
)

var _ = BeforeEach(func() {
	utils.System = utils.InitializeSystemFunctions()
})

func TestCommands(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Config Suite")
}
