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
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("hub", func() {
	Describe("check config internals", func() {

		var (
			saveHomeDir string
		)

		BeforeEach(func() {
			saveHomeDir = testUtils.ResetTempHomeDir()
		})

		AfterEach(func() {
			os.Setenv("HOME", saveHomeDir)
		})

		Describe("happy: the database is running, master-host is provided, and connection is successful", func() {
			It("writes a file to ~/.gp_upgrade/cluster_config.json with correct json", func() {
				dbConnector, mock := db.CreateMockDBConn()
				dbConnector.Connect()
				fakeQuery := "SELECT barCol FROM foo"

				header := []string{"dbid", "content", "role", "preferred_role", "mode", "status", "port",
					"hostname", "address", "replication_port", "san_mounts",
					"fsefsoid", "fsedbid", "fselocation"}
				fakeConfigRow := []driver.Value{1, -1, 'p', 'p', 's', 'u', 15432, "office-5-231.pa.pivotal.io",
					"office-5-231.pa.pivotal.io", nil, nil, nil, nil, nil}
				fakeConfigRow2 := []driver.Value{2, 0, 'p', 'p', 's', 'u', 25432, "office-5-231.pa.pivotal.io",
					"office-5-231.pa.pivotal.io", 25438, nil, nil, nil, nil}
				rows := sqlmock.NewRows(header)
				heapfakeResult := rows.AddRow(fakeConfigRow...).AddRow(fakeConfigRow2...)
				mock.ExpectQuery(fakeQuery).WillReturnRows(heapfakeResult)

				err := services.CreateConfigurationFile(dbConnector.GetConn(), fakeQuery, config.NewWriter())

				Expect(err).ToNot(HaveOccurred())

				// No controller test up into which to pull this assertion
				// So maybe look into putting assertions like this into the integration tests, so protect against leaks?
				dbConnector.Close()
				Expect(dbConnector.GetConn().Stats().OpenConnections).To(Equal(0))
				content, err := ioutil.ReadFile(config.GetConfigFilePath())
				testUtils.Check("cannot read file", err)

				resultData := make([]map[string]interface{}, 0)
				expectedData := make([]map[string]interface{}, 0)
				json.Unmarshal(content, resultData)
				json.Unmarshal([]byte(EXPECTED_CHECK_CONFIGURATION_OUTPUT), expectedData)
				Expect(expectedData).To(Equal(resultData))
			})
		})

		Describe("errors", func() {
			Describe("when the query fails", func() {

				It("returns an error", func() {
					dbConnector, mock := db.CreateMockDBConn()
					fakeFailingQuery := "SEJECT % ofrm tabel1"
					mock.ExpectQuery(fakeFailingQuery).WillReturnError(errors.New("the query has failed"))
					dbConnector.Connect()

					err := services.CreateConfigurationFile(dbConnector.GetConn(), fakeFailingQuery, config.NewWriter())
					Expect(err).To(HaveOccurred())

					// No controller test up into which to pull this assertion
					// So maybe look into putting assertions like this into the integration tests, so protect against leaks?
					dbConnector.Close()
					Expect(dbConnector.GetConn().Stats().OpenConnections).To(Equal(0))
				})
			})
			Describe("when the home directory is not writable", func() {
				It("returns an error", func() {
					dbConnector, mock := db.CreateMockDBConn()
					dbConnector.Connect()

					// refactor: use same logic as happy path
					minimalSucceedingFakeQuery := "SELECT fooCol FROM bar"
					header := []string{"dbid", "content", "role", "preferred_role", "mode", "status", "port",
						"hostname", "address", "replication_port", "san_mounts",
						"fsefsoid", "fsedbid", "fselocation"}
					fakeConfigRow := []driver.Value{1, -1, 'p', 'p', 's', 'u', 15432, "office-5-231.pa.pivotal.io",
						"office-5-231.pa.pivotal.io", nil, nil, nil, nil, nil}
					fakeConfigRow2 := []driver.Value{2, 0, 'p', 'p', 's', 'u', 25432, "office-5-231.pa.pivotal.io",
						"office-5-231.pa.pivotal.io", 25438, nil, nil, nil, nil}
					rows := sqlmock.NewRows(header)
					heapfakeResult := rows.AddRow(fakeConfigRow...).AddRow(fakeConfigRow2...)
					mock.ExpectQuery(minimalSucceedingFakeQuery).WillReturnRows(heapfakeResult)

					err := os.MkdirAll(config.GetConfigDir(), 0500)
					testUtils.Check("cannot chmod: ", err)

					err = services.CreateConfigurationFile(dbConnector.GetConn(), minimalSucceedingFakeQuery, config.NewWriter())
					dbConnector.Close()
					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("open /tmp/gp_upgrade_test_temp_home_dir/.gp_upgrade/cluster_config.json: permission denied"))
				})
			})

			Describe("when the writer fails at parsing the db result", func() {
				It("returns an error", func() {

					dbConnector, mock := db.CreateMockDBConn()
					dbConnector.Connect()
					//MasterHost = "localhost"

					fineFakeQuery := "SELECT fooCol FROM bar"
					header := []string{"dbid", "content", "role", "preferred_role", "mode", "status", "port",
						"hostname", "address", "replication_port", "san_mounts",
						"fsefsoid", "fsedbid", "fselocation"}
					fakeConfigRow := []driver.Value{1, -1, 'p', 'p', 's', 'u', 15432, "office-5-231.pa.pivotal.io",
						"office-5-231.pa.pivotal.io", nil, nil, nil, nil, nil}
					fakeConfigRow2 := []driver.Value{2, 0, 'p', 'p', 's', 'u', 25432, "office-5-231.pa.pivotal.io",
						"office-5-231.pa.pivotal.io", 25438, nil, nil, nil, nil}
					rows := sqlmock.NewRows(header)
					arbitraryFakeResult := rows.AddRow(fakeConfigRow...).AddRow(fakeConfigRow2...)
					mock.ExpectQuery(fineFakeQuery).WillReturnRows(arbitraryFakeResult)

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

func setupSegmentConfigInDB(mock sqlmock.Sqlmock) {
	header := []string{"dbid", "content", "role", "preferred_role", "mode", "status", "port",
		"hostname", "address", "replication_port", "san_mounts",
		"fsefsoid", "fsedbid", "fselocation"}
	fakeConfigRow := []driver.Value{1, -1, 'p', 'p', 's', 'u', 15432, "office-5-231.pa.pivotal.io",
		"office-5-231.pa.pivotal.io", nil, nil, nil, nil, nil}
	fakeConfigRow2 := []driver.Value{2, 0, 'p', 'p', 's', 'u', 25432, "office-5-231.pa.pivotal.io",
		"office-5-231.pa.pivotal.io", 25438, nil, nil, nil, nil}
	rows := sqlmock.NewRows(header)
	heapfakeResult := rows.AddRow(fakeConfigRow...).AddRow(fakeConfigRow2...)
	mock.ExpectQuery(SELECT_SEGMENT_CONFIG_QUERY).WillReturnRows(heapfakeResult)
}

const (
	EXPECTED_CHECK_CONFIGURATION_OUTPUT = `[
	{
	  "address": "office-5-231.pa.pivotal.io",
	  "content": -1,
	  "dbid": 1,
	  "fsedbid": null,
	  "fsefsoid": null,
	  "fselocation": null,
	  "hostname": "office-5-231.pa.pivotal.io",
	  "mode": 115,
	  "port": 15432,
	  "preferred_role": 112,
	  "replication_port": null,
	  "role": 112,
	  "san_mounts": null,
	  "status": 117
	},
	{
	  "address": "office-5-231.pa.pivotal.io",
	  "content": 0,
	  "dbid": 2,
	  "fsedbid": null,
	  "fsefsoid": null,
	  "fselocation": null,
	  "hostname": "office-5-231.pa.pivotal.io",
	  "mode": 115,
	  "port": 25432,
	  "preferred_role": 112,
	  "replication_port": 25438,
	  "role": 112,
	  "san_mounts": null,
	  "status": 117
	}
	]`

	SELECT_SEGMENT_CONFIG_QUERY = "select dbid, content.*"
)
