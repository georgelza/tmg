/*
*
*	File		: server.go
*
* 	Created		: 8 Apr 2021, Sourced from poster_term.go
*
*	Description	: Started from Poster_term, modified to store data into a Cassandra DB.
*
*	Modified	: 8 April 2021	Started
*				:
*
*	By			: George Leonard (georgelza@gmail.com)
*
*	Cassandra
*	https://www.instaclustr.com/support/documentation/cassandra/using-cassandra/connect-to-cassandra-with-golang/
* 	https://getstream.io/blog/building-a-performant-api-using-go-and-cassandra/
*	https://golangdocs.com/golang-cassandra-example
*	https://play.golang.org/p/4U7bx564d5
*
*	https://github.com/CaiqueCosta/YouTube-Tutorials/tree/master/FirstApp
*
*
*	GOOD Cassandra DB Package / https://github.com/mailgun/sandra/blob/master/cassandra.go
*
 */

package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/georgelza/tmg_poster_cas/database"
	"github.com/georgelza/tmg_poster_cas/person"

	"google.golang.org/grpc"
	glog "google.golang.org/grpc/grpclog"
)

var grpcLog glog.LoggerV2

func init() {

	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

}

func main() {

	fmt.Println("")
	fmt.Println("###############################################################")
	fmt.Println("#")
	fmt.Println("# 	File 		: poster_cas.go ")
	fmt.Println("#")
	fmt.Println("#	Comment		: Recieves data from scrubber as a gRPC/Protobuf msg")
	fmt.Println("#	 			: & persists it into Cassandra DB")
	fmt.Println("#")
	fmt.Println("# 	By  		: George Leonard (georgelza@gmail.com)")
	fmt.Println("#")
	fmt.Println("# 	ate/Time 	:", time.Now().Format("02-01-2006 - 15:04:05"))
	fmt.Println("#")
	fmt.Println("###############################################################")
	fmt.Println("")

	// Lets identify ourself
	var vHostname, e2 = os.Hostname()
	if e2 != nil {
		grpcLog.Error("Can't retrieve hostname", e2)
	}

	// Reading variables using the model - from either shell script exports or configmap in K8s deployment
	grpcLog.Info("Retrieving variables ..")

	var vGRPC_Server = os.Getenv("GRPC_SERVER_NAME")
	var vGRPC_Port = os.Getenv("GRPC_SERVER_PORT")

	var vDatabase_username = os.Getenv("DATABASE_USERNAME")
	var vDatabase_password = os.Getenv("DATABASE_PASSWORD")
	var vDatabase_host = os.Getenv("DATABASE_DBHOST")
	var vDatabase_port = os.Getenv("DATABASE_DBPORT")
	var vDatabase_keyspace = os.Getenv("DATABASE_DBKEYSPACE")

	var vDebug_Level = os.Getenv("DEBUGLEVEL")

	grpcLog.Info("Hostname is\t\t", vHostname)

	grpcLog.Info("gRPC Port is\t\t", vGRPC_Port)
	grpcLog.Info("gRPC Name is\t\t", vGRPC_Server)

	grpcLog.Info("DB Host is\t\t", vDatabase_host)
	grpcLog.Info("DB Port is\t\t", vDatabase_port)
	grpcLog.Info("DB Keyspace is\t", vDatabase_keyspace)
	grpcLog.Info("DB Username is\t", vDatabase_username)
	grpcLog.Info("DB Password is\t", vDatabase_password)

	grpcLog.Info("Debug Level is\t", vDebug_Level)

	// Lets manage how much we prnt to the screen
	var vDebugLevel, e1 = strconv.Atoi(vDebug_Level)
	if e1 != nil {
		grpcLog.Error("Debug Level: String to Int convert error: %s", e1)

	}

	grpcLog.Info("")
	grpcLog.Info("**** Configure gRPC Connection ****")

	// Setup gRPC Server environment
	listener, err := net.Listen("tcp", vGRPC_Server+":"+vGRPC_Port)
	if err != nil {
		grpcLog.Errorf("Failed to serve: %s", err)
		os.Exit(1)

	}
	grpcLog.Info("Create a listener on TCP port :", vGRPC_Port)

	server := person.Server{}
	grpcLog.Info("Create a server instance")

	grpcServer := grpc.NewServer()
	grpcLog.Info("Create a gRPC server object")

	person.RegisterDataSaverServer(grpcServer, &server)
	grpcLog.Info("Attach the DataSaver service to the server")

	grpcLog.Info("")
	grpcLog.Info("**** Configure Cassandra Connection ****")

	// Lets store some of these system wide values in my server object
	server.Debug_level = vDebugLevel

	// Lets identify ourself
	server.Hostname = vHostname

	// Setup Cassandra Database connect/Server environment
	server.DBConn = database.SetupDBConnection(vDatabase_keyspace, vDatabase_username, vDatabase_password, vDatabase_port, vDatabase_host)
	if server.DBConn == nil {
		grpcLog.Errorf("Problem initialising database connection:")
		os.Exit(1)

	}
	defer server.DBConn.Close()
	grpcLog.Info("Connected to Cassandra Server")

	grpcLog.Info("")

	if err := grpcServer.Serve(listener); err != nil {
		grpcLog.Info("Failed to serve: %s", err)

	}

}
