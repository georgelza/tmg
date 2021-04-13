/*
*
*
*
*
*
*	https://github.com/CaiqueCosta
*
 */

package database

import (
	"fmt"
	"os"
	"time"

	"github.com/gocql/gocql"
	glog "google.golang.org/grpc/grpclog"
)

var grpcLog glog.LoggerV2

func init() {

	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

}

type DBConnection struct {
	session *gocql.Session
}

func SetupDBConnection(keyspace string, username string, password string, dbport string, dbhosts string) *DBConnection {

	cluster := gocql.NewCluster(dbhosts)
	grpcLog.Infof("Setting up Cassandra DBConnection")

	cluster.Consistency = gocql.Quorum
	grpcLog.Infof("Configuring Cassandra Quorum")

	cluster.ConnectTimeout = time.Second * 10
	grpcLog.Infof("Configuring Cassandra connect timeout")

	cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password} //replace the username and password fields with their real settings.
	grpcLog.Infof("Configuring Cassandra Authenticator")

	cluster.Keyspace = keyspace
	grpcLog.Infof("Configuring Cassandra Keyspace")

	// Configure Protocol version to utilise
	cluster.ProtoVersion = 4

	session, err := cluster.CreateSession()
	if err != nil {
		grpcLog.Fatalf("Failed to Cassandra DB session: %v", err)
		return nil
	}
	grpcLog.Infof("Created Cassandra DB Server session")

	return &DBConnection{session: session}
}

func (db *DBConnection) ExecuteQuery(query string, values ...interface{}) error {
	if err := db.session.Query(query).Bind(values...).Exec(); err != nil {
		grpcLog.Fatalf("Error Executing Query : %s", fmt.Sprintf("%s: Error: %s", query, err))

		return err
	}
	return nil
}

func (db *DBConnection) Close() {

	db.session.Close()

}
