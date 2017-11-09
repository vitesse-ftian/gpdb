package services_test

import (
	"gp_upgrade/config"
	"gp_upgrade/db"
	"gp_upgrade/hub/services"
	"gp_upgrade/idl"
	"gp_upgrade/testUtils"
	"gp_upgrade/utils"

	"io/ioutil"
	"os"

	"database/sql/driver"
	"encoding/json"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var _ = Describe("hub", func() {
	Describe("creates a reply", func() {
		It("Confirms that config information retrieved and sent", func() {
			listener := services.NewCliToHubListener()

			services.CreateConfigFile = func(db.Connector, config.Store) error {
				return nil
			}

			fakeCheckConfigRequest := &idl.CheckConfigRequest{DbPort: 9999}
			formulatedResponse, err := listener.CheckConfig(nil, fakeCheckConfigRequest)
			Expect(err).To(BeNil())
			Expect(formulatedResponse.ConfigStatus).Should(Equal("All good"))
		})

	})
	Describe("check config internals", func() {

		var (
			saveHomeDir string
		)

		BeforeEach(func() {
			saveHomeDir = testUtils.ResetTempHomeDir()
			services.CreateConfigFile = func(db.Connector, config.Store) error {
				return nil
			}
		})

		AfterEach(func() {
			os.Setenv("HOME", saveHomeDir)
		})

		Describe("happy: the database is running, master-host is provided, and connection is successful", func() {
			It("writes a file to ~/.gp_upgrade/cluster_config.json with correct json", func() {
				dbConnector, mock := db.CreateMockDBConn()
				setupSegmentConfigInDB(mock)
				err := services.CreateConfigurationFile(dbConnector, config.NewWriter())

				Expect(err).ToNot(HaveOccurred())
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
			Describe("when the query fails on AO table count", func() {

				It("returns an error", func() {
					dbConnector, mock := db.CreateMockDBConn()
					mock.ExpectQuery(SELECT_SEGMENT_CONFIG_QUERY).WillReturnError(errors.New("the query has failed"))

					err := services.CreateConfigurationFile(dbConnector, config.NewWriter())

					Expect(dbConnector.GetConn().Stats().OpenConnections).To(Equal(0))
					Expect(err).To(HaveOccurred())
				})
			})
			Describe("when the db dbConn fails", func() {
				It("returns an error", func() {
					failingDbConnector := FailingDbConnector{}
					err := services.CreateConfigurationFile(failingDbConnector, nil)

					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("Invalid DB Connection"))
				})
			})
			Describe("when the home directory is not writable", func() {
				It("returns an error", func() {
					dbConnector, mock := db.CreateMockDBConn()
					setupSegmentConfigInDB(mock)
					err := os.MkdirAll(config.GetConfigDir(), 0500)
					testUtils.Check("cannot chmod: ", err)

					err = services.CreateConfigurationFile(dbConnector, config.NewWriter())

					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("open /tmp/gp_upgrade_test_temp_home_dir/.gp_upgrade/cluster_config.json: permission denied"))
				})
			})

			Describe("when db result cannot be parsed", func() {
				It("returns an error", func() {

					dbConnector, mock := db.CreateMockDBConn()
					setupSegmentConfigInDB(mock)
					setupSegmentConfigInDB(mock)
					mock.ExpectQuery(SELECT_SEGMENT_CONFIG_QUERY).WillReturnError(errors.New("the query has failed"))
					//MasterHost = "localhost"

					fake := FakeWriter{}
					err := services.CreateConfigurationFile(dbConnector, fake)

					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("I always fail"))
				})
			})
		})
	})
})

type FakeWriter struct{}

func (FakeWriter) Load(rows utils.RowsWrapper) error {
	return errors.New("I always fail")
}

func (FakeWriter) Write() error {
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

type FailingDbConnector struct{}

func (FailingDbConnector) Connect() error {
	return errors.New("Invalid DB Connection")
}
func (FailingDbConnector) Close() {
}
func (FailingDbConnector) GetConn() *sqlx.DB {
	return nil
}
