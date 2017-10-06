package commands

import (
	"database/sql/driver"

	"github.com/pkg/errors"

	"gp_upgrade/db"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var (
	subject ObjectCountCommand
)

var _ = Describe("object count tests", func() {
	BeforeEach(func() {
		subject = NewObjectCountCommand("", 0, FakeDbConnectionFactory{})
	})

	Describe("check object count", func() {
		Describe("happy", func() {
			// todo test Execute since we have a factory for dbconnect now

			It("prints the number of AO and heap objects in the database", func() {
				dbConn, mock := db.CreateMockDBConn()
				AddObjectCountMock(mock)
				buffer := gbytes.NewBuffer()
				err := subject.executeSingleDatabase(dbConn, buffer)

				buffer.Close()
				Expect(err).ToNot(HaveOccurred())
				Expect(dbConn.GetConn().Stats().OpenConnections).To(Equal(0))
				Expect(string(buffer.Contents())).To(ContainSubstring("Number of AO objects - 5"))
				Expect(string(buffer.Contents())).To(ContainSubstring("Number of heap objects - 10"))
			})
			It("ALL: prints the number of AO and heap objects in multiple databases", func() {
				dbConn, mock := db.CreateMockDBConn()
				AddCountDatabasesMock(mock)
				buffer := gbytes.NewBuffer()
				subject.dbConnFactory.NewDBConn("", 0, "test")
				addMockFunction = AddObjectCountMock
				err := subject.executeAll(dbConn, buffer)

				buffer.Close()
				Expect(err).ToNot(HaveOccurred())

			})
		})

		Describe("errors", func() {
			Describe("when the query fails on AO table count", func() {

				It("returns an error", func() {
					dbConnector, mock := db.CreateMockDBConn()
					mock.ExpectQuery("SELECT COUNT.*AND c.relstorage IN.*").WillReturnError(errors.New("pq: the query for AO table count has failed"))

					err := subject.executeSingleDatabase(dbConnector, nil)

					Expect(dbConnector.GetConn().Stats().OpenConnections).To(Equal(0))
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(errors.New("ERROR: [check object-count] the query for AO table count has failed")))

				})
			})
			Describe("when the query fails on heap-only table count", func() {

				It("returns an error", func() {
					dbConnector, mock := db.CreateMockDBConn()
					mock.ExpectQuery("").WillReturnError(errors.New("pq: the query for heap-only table count has failed"))

					err := subject.executeSingleDatabase(dbConnector, nil)

					Expect(dbConnector.GetConn().Stats().OpenConnections).To(Equal(0))
					Expect(err).To(HaveOccurred())
					Expect(err).To(Equal(errors.New("ERROR: [check object-count] the query for heap-only table count has failed")))

				})
			})
			Describe("when the db dbConn fails", func() {
				It("returns an error", func() {

					err := subject.executeSingleDatabase(FailingDbConnector{}, nil)

					Expect(err).To(HaveOccurred())
					Expect(err.Error()).To(ContainSubstring("Invalid DB Connection"))
				})
			})
		})
	})
})
var databaseConnectionMock sqlmock.Sqlmock
var dbConnector db.Connector
var addMockFunction = func(mock sqlmock.Sqlmock) {}

type FakeDbConnectionFactory struct{}

func (FakeDbConnectionFactory) NewDBConn(masterHost string, masterPort int, dbname string) db.Connector {
	dbConnector, databaseConnectionMock = db.CreateMockDBConn()
	addMockFunction(databaseConnectionMock)
	return dbConnector
}

func AddObjectCountMock(mock sqlmock.Sqlmock) {
	header := []string{"COUNT"}
	aoObjectCountRow := []driver.Value{5}
	heapObjectCountRow := []driver.Value{10}
	heapfakeResult := sqlmock.NewRows(header).AddRow(heapObjectCountRow...)
	aofakeResult := sqlmock.NewRows(header).AddRow(aoObjectCountRow...)

	mock.ExpectQuery("SELECT COUNT.*AND c.relstorage IN").WillReturnRows(aofakeResult)
	mock.ExpectQuery("SELECT COUNT.*AND c.relstorage NOT IN").WillReturnRows(heapfakeResult)
}

func AddCountDatabasesMock(mock sqlmock.Sqlmock) {
	var datnameRow = []driver.Value{"test"}
	var datnamefakeResult = sqlmock.NewRows([]string{"datname"}).AddRow(datnameRow...).AddRow(datnameRow...)
	mock.ExpectQuery("SELECT datname FROM pg_database WHERE datname != 'template0").WillReturnRows(datnamefakeResult)
}
