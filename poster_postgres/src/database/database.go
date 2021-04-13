/*
*
*
*
*
*
*	https://www.calhoun.io/connecting-to-a-postgresql-database-with-gos-database-sql-package/
*
 */

package database

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq"
	glog "google.golang.org/grpc/grpclog"
)

var grpcLog glog.LoggerV2

func init() {

	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

}

//this is still all Cassandra based, to be refactored into PostgreSQL language
type DBConnection struct {
	session *sql.DB
}

/*
const (
	host   = "localhost"
	port   = 5432
	user   = "postgres"
	dbname = "json"
)
*/

func SetupDBConnection(dbname string, username string, password string, dbport int, dbhost string) *DBConnection {

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"dbname=%s sslmode=disable",
		dbhost, dbport, username, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		grpcLog.Fatalf("Error validating PostgreSQL database params : %s", fmt.Sprintf("%s: Error: %s", dbname, err))

	}

	err = db.Ping()
	if err != nil {
		grpcLog.Fatalf("Error opening connection to PostgreSQL database : %s", fmt.Sprintf("%s: Error: %s", dbname, err))

	}
	grpcLog.Infof("Successfully connected!")

	return &DBConnection{session: db}
}

func (db *DBConnection) ExecuteInsert(query string, values ...interface{}) error {

	if _, err := db.session.Exec(query, values...); err != nil {

		grpcLog.Fatalf("Error Executing Insert : %s", fmt.Sprintf("%s: Error: %s", query, err))

		return err
	}

	return nil
}

func (db *DBConnection) Close() {

	db.session.Close()

}
