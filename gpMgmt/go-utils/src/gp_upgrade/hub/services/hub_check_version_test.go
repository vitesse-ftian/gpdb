package services_test

import (
	"database/sql/driver"
	"errors"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"gp_upgrade/db"
	"gp_upgrade/hub/services"
)

var _ bool = Describe("hub", func() {
	var (
		dbConnector db.Connector
		mock        sqlmock.Sqlmock
	)

	BeforeEach(func() {
		dbConnector, mock = db.CreateMockDBConn()
		dbConnector.Connect()
	})

	AfterEach(func() {
		dbConnector.Close()
		// No controller test up into which to pull this assertion
		// So maybe look into putting assertions like this into the integration tests, so protect against leaks?
		Expect(dbConnector.GetConn().Stats().OpenConnections).
			To(Equal(0))
	})
	Describe("VerifyVersion", func() {
		It("reports that version is compatible", func() {

			mock.ExpectQuery(services.VERSION_QUERY).
				WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow([]driver.Value{VERSION_RESULT}...))
			versionCheckOk, err := services.VerifyVersion(dbConnector.GetConn())
			Expect(err).ToNot(HaveOccurred())
			Expect(versionCheckOk).To(Equal(true))
		})
		It("could not run version query", func() {

			mock.ExpectQuery(services.VERSION_QUERY).
				WillReturnError(errors.New("couldn't connect to db"))
			_, err := services.VerifyVersion(dbConnector.GetConn())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("couldn't connect to db"))
		})
		It("select VERSION() query didn't work", func() {
			mock.ExpectQuery(services.VERSION_QUERY).
				WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow([]driver.Value{"not a good result for version query"}...))
			_, err := services.VerifyVersion(dbConnector.GetConn())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("Didn't get a version string match"))
		})
		It("converting version string to Version object fails", func() {
			mock.ExpectQuery(services.VERSION_QUERY).
				WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow([]driver.Value{BAD_VERSION_RESULT}...))
			_, err := services.VerifyVersion(dbConnector.GetConn())
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("Expected version to be non-empty string"))
		})
		It("reports that version is incompatible", func() {

			mock.ExpectQuery(services.VERSION_QUERY).
				WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow([]driver.Value{INCOMPATIBLE_VERSION_RESULT}...))
			versionCheckOk, err := services.VerifyVersion(dbConnector.GetConn())
			Expect(err).ToNot(HaveOccurred())
			Expect(versionCheckOk).To(Equal(false))
		})
	})
})

const (
	VERSION_RESULT              = `PostgreSQL 8.4devel (Greenplum Database 6.0.0-alpha.0+dev.159.gf2010f7ef4 build dev) on x86_64-apple-darwin16.7.0, compiled by GCC Apple LLVM version 8.1.0 (clang-802.0.42) compiled on Nov 3 2017 14:25:11`
	BAD_VERSION_RESULT          = `PostgreSQL 8.4devel (Greenplum Database  build dev) on x86_64-apple-darwin16.7.0, compiled by GCC Apple LLVM version 8.1.0 (clang-802.0.42) compiled on Nov 3 2017 14:25:11`
	INCOMPATIBLE_VERSION_RESULT = `PostgreSQL 8.4devel (Greenplum Database 4.3.8-alpha.0+dev.159.gf2010f7ef4 build dev) on x86_64-apple-darwin16.7.0, compiled by GCC Apple LLVM version 8.1.0 (clang-802.0.42) compiled on Nov 3 2017 14:25:11`
)
