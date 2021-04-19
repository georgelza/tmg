/*
*
*	File		: server.go
*
* 	Created		: 8 Apr 2021, Sourced from poster_cas.go
*
*	Description	: Started from Poster_cas, modified to store data into a PostgreSQL DB.
*
*	Modified	: 13 April 2021	Started
*				:
*
*	By			: George Leonard (georgelza@gmail.com)
*
*	PostgreSQL
*
*
*
*
 */

package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/georgelza/tmg_poster_postgres/database"
	"github.com/georgelza/tmg_poster_postgres/person"

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
	fmt.Println("#   File      : poster_postgres.go ")
	fmt.Println("#")
	fmt.Println("#	Comment		: Recieves data from scrubber as a gRPC/Protobuf msg")
	fmt.Println("#	 			: & persists it into PostgreSQL DB")
	fmt.Println("#")
	fmt.Println("#   By        : George Leonard (georgelza@gmail.com)")
	fmt.Println("#")
	fmt.Println("#   Date/Time :", time.Now().Format("02-01-2006 - 15:04:05"))
	fmt.Println("#")
	fmt.Println("###############################################################")
	fmt.Println("")

	/***** Retrieve variables from config.yml *****/

	// Lets identify ourself
	var vHostname, err = os.Hostname()
	if err != nil {
		grpcLog.Error("Can't retrieve hostname", err)
	}

	// Reading variables using the model - from either shell script exports or configmap in K8s deployment
	grpcLog.Info("Retrieving variables ..")

	var vGRPC_Server = os.Getenv("GRPC_SERVER_NAME")
	var vGRPC_Port = os.Getenv("GRPC_SERVER_PORT")

	var vDatabase_username = os.Getenv("DATABASE_USERNAME")
	var vDatabase_password = os.Getenv("DATABASE_PASSWORD")
	var vDatabase_host = os.Getenv("DATABASE_HOST")
	var vDatabase_port = os.Getenv("DATABASE_PORT")
	var vDatabase_name = os.Getenv("DATABASE_NAME")

	var vDebug_Level = os.Getenv("DEBUGLEVEL")

	grpcLog.Info("Hostname is\t\t", vHostname)

	grpcLog.Info("gRPC Server is\t", vGRPC_Server)
	grpcLog.Info("gRPC Port is\t\t", vGRPC_Port)

	grpcLog.Info("DB Host is\t\t", vDatabase_host)
	grpcLog.Info("DB Port is\t\t", vDatabase_port)
	grpcLog.Info("DB Name is\t\t", vDatabase_name)
	grpcLog.Info("DB Username is\t", vDatabase_username)
	grpcLog.Info("DB Password is\t", vDatabase_password)

	grpcLog.Info("Debug Level is\t", vDebug_Level)

	// Lets manage how much we prnt to the screen
	var vDebugLevel, e2 = strconv.Atoi(vDebug_Level)
	if e2 != nil {
		grpcLog.Error("Debug Level: String to Int convert error: %s", e2)

	}

	var vDatabaseport, e3 = strconv.Atoi(vDatabase_port)
	if e3 != nil {
		grpcLog.Error("Debug Level: String to Int convert error: %s", e3)

	}

	grpcLog.Info("")
	grpcLog.Info("**** Configure gRPC Connection ****")

	// Setup gRPC Server environment
	listener, err := net.Listen("tcp", vGRPC_Server+":"+vGRPC_Port)
	if err != nil {
		grpcLog.Errorf("Failed to serve: %s", err)

	}
	grpcLog.Info("Create a listener on TCP port :", vGRPC_Port)

	server := person.Server{}
	grpcLog.Info("Create a server instance")

	grpcServer := grpc.NewServer()
	grpcLog.Info("Create a gRPC server object")

	person.RegisterDataSaverServer(grpcServer, &server)
	grpcLog.Info("Attach the DataSaver service to the server")

	grpcLog.Info("")
	grpcLog.Info("**** Configure PostgreSQL Connection ****")

	// Lets store some of these system wide values in my server object
	server.Debug_level = vDebugLevel

	// Lets identify ourself
	server.Hostname = vHostname

	// Setup PostgreSQL Database connect/Server environment
	server.DBConn = database.SetupDBConnection(vDatabase_name, vDatabase_username, vDatabase_password, vDatabaseport, vDatabase_host)
	if server.DBConn == nil {
		grpcLog.Errorf("Problem initialising database connection:")

	}
	defer server.DBConn.Close()
	grpcLog.Info("Connected to PostgreSQL Server")

	grpcLog.Info("")

	if err := grpcServer.Serve(listener); err != nil {
		grpcLog.Info("Failed to serve: %s", err)

	}

}
