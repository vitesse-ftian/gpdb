package services_test

import (
	"errors"
	pb "gp_upgrade/idl"
	"gp_upgrade/utils"
	"os"
	"strings"

	"github.com/greenplum-db/gpbackup/testutils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gp_upgrade/hub/services"
	"io/ioutil"
	"time"
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

		It("reports that master upgrade is pending when pg_upgrade dir does not exist", func() {
			listener := services.NewCliToHubListener()
			var fakeStatusUpgradeRequest *pb.StatusUpgradeRequest

			utils.System.IsNotExist = func(error) bool {
				return true
			}

			formulatedResponse, err := listener.StatusUpgrade(nil, fakeStatusUpgradeRequest)
			Expect(err).To(BeNil())

			stepStatuses := formulatedResponse.GetListOfUpgradeStepStatuses()

			for _, stepStatus := range stepStatuses {
				if stepStatus.GetStep() == pb.UpgradeSteps_MASTERUPGRADE {
					Expect(stepStatus.GetStatus()).To(Equal(pb.StepStatus_PENDING))
				}
			}
		})
		It("reports that master upgrade is running when pg_upgrade/*.inprogress files exists", func() {
			listener := services.NewCliToHubListener()
			var fakeStatusUpgradeRequest *pb.StatusUpgradeRequest

			utils.System.IsNotExist = func(error) bool {
				return false
			}
			utils.System.FilePathGlob = func(string) ([]string, error) {
				return []string{"somefile.inprogress"}, nil
			}

			formulatedResponse, err := listener.StatusUpgrade(nil, fakeStatusUpgradeRequest)
			Expect(err).To(BeNil())

			stepStatuses := formulatedResponse.GetListOfUpgradeStepStatuses()

			for _, stepStatus := range stepStatuses {
				if stepStatus.GetStep() == pb.UpgradeSteps_MASTERUPGRADE {
					Expect(stepStatus.GetStatus()).To(Equal(pb.StepStatus_RUNNING))
				}
			}
		})
		It("reports that master upgrade is done when no *.inprogress files exist in ~/.gp_upgrade/pg_upgrade", func() {
			listener := services.NewCliToHubListener()
			var fakeStatusUpgradeRequest *pb.StatusUpgradeRequest

			utils.System.IsNotExist = func(error) bool {
				return false
			}
			utils.System.FilePathGlob = func(glob string) ([]string, error) {
				if strings.Contains(glob, "inprogress") {
					return nil, nil
				} else if strings.Contains(glob, "done") {
					return []string{"found something"}, nil
				}

				return nil, errors.New("Test not configured for this glob.")
			}
			utils.System.Stat = func(filename string) (os.FileInfo, error) {
				if strings.Contains(filename, "found something") {
					return &fakeFileInfo{}, nil
				}
				return nil, nil
			}

			utils.System.Open = func(name string) (*os.File, error) {
				// Temporarily create a file that we can read as a real file descriptor
				fd, err := ioutil.TempFile("/tmp", "hub_status_upgrade_test")
				Expect(err).To(BeNil())

				filename := fd.Name()
				fd.WriteString("12312312;Upgrade complete;\n")
				fd.Close()
				return os.Open(filename)

			}

			formulatedResponse, err := listener.StatusUpgrade(nil, fakeStatusUpgradeRequest)
			Expect(err).To(BeNil())

			stepStatuses := formulatedResponse.GetListOfUpgradeStepStatuses()

			for _, stepStatus := range stepStatuses {
				if stepStatus.GetStep() == pb.UpgradeSteps_MASTERUPGRADE {
					Expect(stepStatus.GetStatus()).To(Equal(pb.StepStatus_COMPLETE))
				}
			}
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

type fakeFileInfo struct {
	name    string      // base name of the file
	size    int64       // length in bytes for regular files; system-dependent for others
	mode    os.FileMode // file mode bits
	modTime time.Time   // modification time
	isDir   bool        // abbreviation for Mode().IsDir()
	sys     interface{}
}

func (f fakeFileInfo) Name() string {
	return f.name
}

func (f fakeFileInfo) Size() int64 {
	return f.size
}
func (f fakeFileInfo) Mode() os.FileMode {
	return f.mode
}
func (f fakeFileInfo) ModTime() time.Time {
	return f.modTime
}
func (f fakeFileInfo) IsDir() bool {
	return f.isDir
}

func (f fakeFileInfo) Sys() interface{} {
	return nil
}
