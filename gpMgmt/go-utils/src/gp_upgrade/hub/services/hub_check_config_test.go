package services_test

import (
	"gp_upgrade/config"
	"gp_upgrade/db"
	"gp_upgrade/hub/services"
	"gp_upgrade/testUtils"
	"gp_upgrade/utils"

	"io/ioutil"
	"os"

	"database/sql/driver"
	"encoding/json"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/pkg/errors"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("hub", func() {
	Describe("check config internals", func() {

		var (
			saveHomeDir string
			dbConnector db.Connector
			mock        sqlmock.Sqlmock
		)

		BeforeEach(func() {
			// remove the homedir logic once we inject a fake writer everywhere
			// will need to inject an arbitrary writer anyway for the different version file destination
			saveHomeDir = testUtils.ResetTempHomeDir()

			dbConnector, mock = db.CreateMockDBConn()
			dbConnector.Connect()
		})

		AfterEach(func() {
			os.Setenv("HOME", saveHomeDir)

			dbConnector.Close()
			// No controller test up into which to pull this assertion
			// So maybe look into putting assertions like this into the integration tests, so protect against leaks?
			Expect(dbConnector.GetConn().Stats().OpenConnections).To(Equal(0))
		})

		Describe("happy: the database is running, master-host is provided, and connection is successful", func() {
			It("writes a file to ~/.gp_upgrade/cluster_config.json with correct json", func() {
				fakeQuery := "SELECT barCol FROM foo"
				mock.ExpectQuery(fakeQuery).WillReturnRows(getHappyFakeRows())

				err := services.CreateConfigurationFile(dbConnector.GetConn(), fakeQuery, config.NewWriter())

				Expect(err).ToNot(HaveOccurred())

				content, err := ioutil.ReadFile(config.GetConfigFilePath())
				testUtils.Check("cannot read file", err)

				resultData := make([]map[string]interface{}, 0)
				expectedData := make([]map[string]interface{}, 0)
				err = json.Unmarshal(content, &resultData)
				Expect(err).ToNot(HaveOccurred())
				err = json.Unmarshal([]byte(EXPECTED_CHECK_CONFIGURATION_OUTPUT), &expectedData)
				Expect(err).ToNot(HaveOccurred())
				Expect(expectedData).To(Equal(resultData))
			})
		})

		Describe("errors", func() {
			Describe("when the query fails", func() {
				It("returns an error", func() {
					fakeFailingQuery := "SEJECT % ofrm tabel1"
					mock.ExpectQuery(fakeFailingQuery).WillReturnError(errors.New("the query has failed"))

					err := services.CreateConfigurationFile(dbConnector.GetConn(), fakeFailingQuery, config.NewWriter())
					Expect(err).To(HaveOccurred())
				})
			})

			Describe("when the home directory is not writable", func() {
				It("returns an error", func() {
					// focus on the write failing rather than querying
					fineFakeQuery := "SELECT fooCol FROM bar"
					mock.ExpectQuery(fineFakeQuery).WillReturnRows(getHappyFakeRows())

					err := os.MkdirAll(config.GetConfigDir(), 0500)
					testUtils.Check("cannot chmod: ", err)

					err = services.CreateConfigurationFile(dbConnector.GetConn(), fineFakeQuery, config.NewWriter())
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("open /tmp/gp_upgrade_test_temp_home_dir/.gp_upgrade/cluster_config.json: permission denied"))
				})
			})

			Describe("when the writer fails at parsing the db result", func() {
				It("returns an error", func() {
					// focus on the writer failing rather than querying
					fineFakeQuery := "SELECT fooCol FROM bar"
					mock.ExpectQuery(fineFakeQuery).WillReturnRows(getHappyFakeRows())

					err := services.CreateConfigurationFile(dbConnector.GetConn(), fineFakeQuery, FailingWriter{})

					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("I always fail"))
				})
			})
		})
	})
})

type FailingWriter struct{}

func (FailingWriter) Load(rows utils.RowsWrapper) error {
	return errors.New("I always fail")
}

func (FailingWriter) Write() error {
	return errors.New("I always fail")
}

// Construct sqlmock in-memory rows to match EXPECTED_CHECK_CONFIGURATION_OUTPUT
func getHappyFakeRows() *sqlmock.Rows {
	header := []string{"dbid", "content", "role", "preferred_role", "mode", "status", "port",
		"hostname", "address", "replication_port", "san_mounts",
		"fsefsoid", "fsedbid", "fselocation"}
	fakeConfigRow := []driver.Value{1, -1, 'p', 'p', 's', 'u', 15432, "mdw.local",
		"mdw.local", nil, nil, nil, nil, nil}
	fakeConfigRow2 := []driver.Value{2, 0, 'p', 'p', 's', 'u', 25432, "sdw1.local",
		"sdw1.local", 25438, nil, nil, nil, nil}
	rows := sqlmock.NewRows(header)
	heapfakeResult := rows.AddRow(fakeConfigRow...).AddRow(fakeConfigRow2...)
	return heapfakeResult
}

const (
	EXPECTED_CHECK_CONFIGURATION_OUTPUT = `[
	{
	  "address": "mdw.local",
	  "content": -1,
	  "dbid": 1,
	  "fsedbid": null,
	  "fsefsoid": null,
	  "fselocation": null,
	  "hostname": "mdw.local",
	  "mode": 115,
	  "port": 15432,
	  "preferred_role": 112,
	  "replication_port": null,
	  "role": 112,
	  "san_mounts": null,
	  "status": 117
	},
	{
	  "address": "sdw1.local",
	  "content": 0,
	  "dbid": 2,
	  "fsedbid": null,
	  "fsefsoid": null,
	  "fselocation": null,
	  "hostname": "sdw1.local",
	  "mode": 115,
	  "port": 25432,
	  "preferred_role": 112,
	  "replication_port": 25438,
	  "role": 112,
	  "san_mounts": null,
	  "status": 117
	}
	]`
)
