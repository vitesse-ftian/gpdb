package configutils_test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"gp_upgrade/testUtils"
	"io/ioutil"
	"os"

	"gp_upgrade/hub/configutils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("configWriter", func() {
	var (
		saved_old_home string
		subject        *configutils.Writer
	)

	BeforeEach(func() {
		saved_old_home = os.Getenv("HOME")
		testUtils.EnsureHomeDirIsTempAndClean()
		subject = configutils.NewWriter("/tmp/doesnotexist")
	})

	AfterEach(func() {
		os.Setenv("HOME", saved_old_home)
	})

	Describe("#Load", func() {
		It("initializes a configuration", func() {
			sampleCombinedRows := make([]interface{}, 2)
			sampleCombinedRows[0] = "value1"
			sampleCombinedRows[1] = []byte{35}
			fakeRows := &testUtils.FakeRows{
				FakeColumns:      []string{"colnameString", "colnameBytes"},
				NumRows:          1,
				SampleRowStrings: sampleCombinedRows,
			}
			err := subject.Load(fakeRows)

			Expect(err).NotTo(HaveOccurred())
			Expect(len(subject.TableJSONData)).To(Equal(1))
			Expect(subject.TableJSONData[0]["colnameString"]).To(Equal("value1"))
			Expect(subject.TableJSONData[0]["colnameBytes"]).To(Equal("#"))
		})
		Describe("error cases", func() {
			It("is returns an error if rows are empty", func() {
				rows := &sql.Rows{}
				err := subject.Load(rows)

				Expect(err).To(HaveOccurred())
			})

			It("returns an error if the given rows do not parse via Columns()", func() {
				var sample []interface{}
				sample = make([]interface{}, 1)

				sample[0] = "value1"
				fakeRows := &testUtils.FakeRows{
					FakeColumns:      []string{"colname1", "colname2"},
					NumRows:          1,
					SampleRowStrings: sample,
				}
				subject := configutils.NewWriter("/tmp/doesnotexist")
				err := subject.Load(fakeRows)
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Describe("#Write", func() {
		const (
			// the output is pretty-printed, so match that format precisely
			expected_json = `[
  {
    "some": "json"
  }
]`
		)

		var (
			json_structure []map[string]interface{}
		)

		BeforeEach(func() {
			err := json.Unmarshal([]byte(expected_json), &json_structure)
			Expect(err).NotTo(HaveOccurred())
		})
		It("writes a configuration when given json", func() {
			subject := configutils.Writer{
				TableJSONData: json_structure,
				Formatter:     configutils.NewJSONFormatter(),
				FileWriter:    configutils.NewRealFileWriter(),
				PathToFile:    configutils.GetConfigFilePath(),
			}
			err := subject.Write()

			Expect(err).NotTo(HaveOccurred())

			content, err := ioutil.ReadFile(configutils.GetConfigFilePath())
			Expect(err).NotTo(HaveOccurred())
			Expect(expected_json).To(Equal(string(content)))
		})
		Describe("error cases", func() {
			It("returns an error when home directory is not writable", func() {
				os.Chmod(testUtils.TempHomeDir, 0100)
				subject := configutils.Writer{
					TableJSONData: json_structure,
					Formatter:     configutils.NewJSONFormatter(),
					FileWriter:    configutils.NewRealFileWriter(),
				}
				err := subject.Write()

				Expect(err).To(HaveOccurred())
				Expect(string(err.Error())).To(ContainSubstring(fmt.Sprintf("mkdir %v/.gp_upgrade: permission denied", testUtils.TempHomeDir)))
			})
			It("returns an error when cluster configutils.go file cannot be opened", func() {
				// pre-create the directory with 0100 perms
				err := os.MkdirAll(configutils.GetConfigDir(), 0100)
				Expect(err).NotTo(HaveOccurred())

				subject := configutils.Writer{
					TableJSONData: json_structure,
					Formatter:     configutils.NewJSONFormatter(),
					PathToFile:    configutils.GetConfigFilePath(),
				}
				err = subject.Write()

				Expect(err).To(HaveOccurred())
				Expect(string(err.Error())).To(ContainSubstring(fmt.Sprintf("open %v/.gp_upgrade/cluster_config.json: permission denied", testUtils.TempHomeDir)))
			})
			It("returns an error when json marshalling fails", func() {
				myMap := make(map[string]interface{})
				myMap["foo"] = make(chan int) // there is no json representation for a channel
				malformed_json_structure := []map[string]interface{}{
					0: myMap,
				}
				subject := configutils.Writer{
					TableJSONData: malformed_json_structure,
					Formatter:     configutils.NewJSONFormatter(),
				}
				err := subject.Write()

				Expect(err).To(HaveOccurred())
			})

			It("returns an error when json pretty print fails", func() {
				subject := configutils.Writer{
					TableJSONData: json_structure,
					Formatter:     &testUtils.ErrorFormatter{},
				}
				err := subject.Write()

				Expect(err).To(HaveOccurred())
			})

			It("returns an error when file writing fails", func() {
				subject := configutils.Writer{
					TableJSONData: json_structure,
					Formatter:     &testUtils.NilFormatter{},
					FileWriter:    &testUtils.ErrorFileWriterDuringWrite{},
				}
				err := subject.Write()

				Expect(err).To(HaveOccurred())
			})

		})
	})
})
