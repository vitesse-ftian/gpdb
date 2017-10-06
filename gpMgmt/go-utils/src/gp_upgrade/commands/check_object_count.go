package commands

import (
	"fmt"

	"io"

	"gp_upgrade/db"
	"os"

	"github.com/pkg/errors"
	"gp_upgrade/utils"
	"strings"
)

type ObjectCountCommand struct {
	MasterHost    string
	MasterPort    int
	dbConnFactory DbConnectionFactory
}

type DbConnectionFactory interface {
	NewDBConn(masterHost string, masterPort int, dbname string) db.Connector
}

func NewObjectCountCommand(masterhost string, port int, factory DbConnectionFactory) ObjectCountCommand {
	return ObjectCountCommand{
		MasterHost: masterhost,
		MasterPort: port,
		dbConnFactory: factory,
	}
}

type RealDbConnectionFactory struct{}

func (RealDbConnectionFactory) NewDBConn(masterHost string, masterPort int, dbname string) db.Connector {
	return db.NewDBConn(masterHost, masterPort, dbname)
}

// The Execute() function connects to the specified host and executes queries to print the number of user Append-Only
// and Heap relations in the "template1" database.
func (cmd ObjectCountCommand) Execute([]string) error {
	dbConn := cmd.dbConnFactory.NewDBConn(cmd.MasterHost, cmd.MasterPort, "template1")
	return cmd.executeAll(dbConn, os.Stdout)
}

func (cmd ObjectCountCommand) executeAll(dbConnector db.Connector, outputWriter io.Writer) error {
	// iterate
	//s := "select datname from pg_database ;";
	err := dbConnector.Connect()
	if err != nil {
		return utils.DatabaseConnectionError{Parent: err}
	}

	connection := dbConnector.GetConn()
	rows, err := connection.Query("SELECT datname FROM pg_database WHERE datname != 'template0'")
	if err != nil {
		errWithoutPq := errors.New(strings.TrimLeft(err.Error(), "pq: "))
		return fmt.Errorf("ERROR: [check object-count] %v", errWithoutPq)
	}

	names := make([]string, 0)

	var name string
	for rows.Next() {
		err = rows.Scan(&name)
		names = append(names, name)
	}
	dbConnector.Close()

	for i := 0; i < len(names); i++ {
		dbConn := cmd.dbConnFactory.NewDBConn(cmd.MasterHost, cmd.MasterPort, names[i])
		err = cmd.executeSingleDatabase(dbConn, outputWriter)
		if err != nil {
			return err
		}
	}

	return nil
}

func (cmd ObjectCountCommand) executeSingleDatabase(dbConnector db.Connector, outputWriter io.Writer) error {

	err := dbConnector.Connect()
	if err != nil {
		return utils.DatabaseConnectionError{Parent: err}
	}
	defer dbConnector.Close()

	err = cmd.executeQuery(dbConnector, AO_CO_TABLE_QUERY_COUNT, "AO", outputWriter)
	if err != nil {
		return err
	}
	err = cmd.executeQuery(dbConnector, HEAP_TABLE_QUERY_COUNT, "heap", outputWriter)

	return err
}

func (cmd ObjectCountCommand) executeQuery(dbConnector db.Connector, query string, objectType string, outputWriter io.Writer) error {
	var count string

	connection := dbConnector.GetConn()
	err := connection.QueryRow(query).Scan(&count)
	if err != nil {
		errWithoutPq := errors.New(strings.TrimLeft(err.Error(), "pq: "))
		return fmt.Errorf("ERROR: [check object-count] %v", errWithoutPq)
	}
	fmt.Fprintf(outputWriter, "Number of %s objects - %v\n", objectType, count)
	return nil
}

const (
	/* "::" casting is specific to Postgres.
	 * changed sql to an ANSI standard casting
		-- COUNT THE NUMBER OF APPEND ONLY OBJECTS ON THE SYSTEM
	*/
	AO_CO_TABLE_QUERY_COUNT = `
	SELECT COUNT(*)
	  FROM pg_class c
	  JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE c.relkind = cast('r' as CHAR)                       -- All tables (including partitions)
	  AND c.relstorage IN ('a','c')                           -- AO / CO
	  AND n.nspname NOT LIKE 'pg_temp_%'                      -- not temp tables
	  AND c.oid >= 16384                                      -- No system tables
	  AND (c.relnamespace >= 16384 OR n.nspname = 'public')   -- No system schemas, but include 'public'
	  AND (NOT relhassubclass                                 -- not partition parent tables
	       OR ( relhassubclass
		    AND NOT EXISTS ( SELECT oid FROM pg_partition_rule p WHERE c.oid = p.parchildrelid )
		    AND NOT EXISTS ( SELECT oid FROM pg_partition p WHERE c.oid = p.parrelid )
		)
	);
	`

	HEAP_TABLE_QUERY_COUNT = `
	SELECT COUNT(*)
	  FROM pg_class c
	  JOIN pg_namespace n ON c.relnamespace = n.oid
	WHERE c.relkind = cast('r' as CHAR)                       -- All tables (including partitions)
	  AND c.relstorage NOT IN ('a','c')                       -- NON AO / CO
	  AND n.nspname NOT LIKE 'pg_temp_%'                      -- not temp tables
	  AND c.oid >= 16384                                      -- No system tables
	  AND (c.relnamespace >= 16384 OR n.nspname = 'public')   -- No system schemas, but include 'public'
	  AND (NOT relhassubclass                                 -- not partition parent tables
	       OR ( relhassubclass
		    AND NOT EXISTS ( SELECT oid FROM pg_partition_rule p WHERE c.oid = p.parchildrelid )
		    AND NOT EXISTS ( SELECT oid FROM pg_partition p WHERE c.oid = p.parrelid )
		)
	  );
	`
)
