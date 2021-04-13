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

	"github.com/georgelza/tmg_poster_cas/config"
	"github.com/georgelza/tmg_poster_cas/database"
	"github.com/georgelza/tmg_poster_cas/person"

	"google.golang.org/grpc"
	glog "google.golang.org/grpc/grpclog"

	"github.com/spf13/viper"
)

var grpcLog glog.LoggerV2

func init() {

	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

}

func main() {

	fmt.Println("")
	fmt.Println("###############################################################")
	fmt.Println("#")
	fmt.Println("#   File      : poster_cas.go ")
	fmt.Println("#")
	fmt.Println("#   By        : George Leonard (georgelza@gmail.com)")
	fmt.Println("#")
	fmt.Println("#   Date/Time :", time.Now().Format("02-01-2006 - 15:04:05"))
	fmt.Println("#")
	fmt.Println("###############################################################")
	fmt.Println("")

	/***** Retrieve variables from config.yml *****/

	/*
		// gRPC Configuration - reintroduce when we go into K8S cluster.
		var vGRPC_Server = os.Getenv("GRPC_SERVER")
		var vGRPC_Port = os.Getenv("GRPC_PORT")
	*/

	// Set the file name of the configurations file
	viper.SetConfigName("config")

	// Set the path to look for the configurations file
	viper.AddConfigPath(".")

	// Enable VIPER to read Environment Variables
	viper.AutomaticEnv()

	viper.SetConfigType("yml")
	var configuration config.Configurations

	if err := viper.ReadInConfig(); err != nil {
		grpcLog.Errorf("Error reading config file, %s", err)
	}

	// Set undefined variables
	viper.SetDefault("database.dbhost", "localhost")
	viper.SetDefault("database.dbport", "9042")

	err := viper.Unmarshal(&configuration)
	if err != nil {
		grpcLog.Errorf("Unable to decode into struct, %v", err)
	}

	// Reading variables using the model
	grpcLog.Info("Retrieved variables using the model..")

	grpcLog.Info("gRPC Port is\t\t", configuration.GRPC_Server.Port)
	grpcLog.Info("gRPC Name is\t\t", configuration.GRPC_Server.Name)
	grpcLog.Info("DB Host is\t\t", configuration.Database.DBHost)
	grpcLog.Info("DB Port is\t\t", configuration.Database.DBPort)
	grpcLog.Info("DB Keyspace is\t", configuration.Database.DBKeyspace)
	grpcLog.Info("DB Username is\t", configuration.Database.Username)
	grpcLog.Info("DB Password is\t", configuration.Database.Password)
	grpcLog.Info("Debug Level is\t", configuration.Debuglevel)

	// Lets manage how much we prnt to the screen
	var vDebugLevel, e1 = strconv.Atoi(configuration.Debuglevel)
	if e1 != nil {
		grpcLog.Error("Debug Level: String to Int convert error: %s", e1)

	}

	grpcLog.Info("")
	grpcLog.Info("**** Configure gRPC Connection ****")

	// Setup gRPC Server environment
	listener, err := net.Listen("tcp", configuration.GRPC_Server.Name+":"+configuration.GRPC_Server.Port)
	if err != nil {
		grpcLog.Errorf("Failed to serve: %s", err)

	}
	grpcLog.Info("Create a listener on TCP port :", configuration.GRPC_Server.Port)

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

	// Setup Cassandra Database connect/Server environment
	server.DBConn = database.SetupDBConnection(configuration.Database.DBKeyspace, configuration.Database.Username, configuration.Database.Password, configuration.Database.DBPort, configuration.Database.DBHost)
	if server.DBConn == nil {
		grpcLog.Errorf("Problem initialising database connection:")

	}
	defer server.DBConn.Close()
	grpcLog.Info("Connected to Cassandra Server")

	grpcLog.Info("")

	if err := grpcServer.Serve(listener); err != nil {
		grpcLog.Info("Failed to serve: %s", err)

	}

}
