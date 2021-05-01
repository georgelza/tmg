/*
*
*	File		: server.go
*
* 	Created		: 8 Apr 2021, Sourced from poster_poster.go
*
*	Description	: Started from Poster_cas, modified to store data into a PostgreSQL DB.
*
*	Modified	: 13 April 2021	Started
*				: 28 April 2021 - Remapping some vatiables so that I have one program, all connecting to multiple databases.
*				: 1 May 2021 	- Moved all the configuration params to a struct for each database.
*
*	By			: George Leonard (georgelza@gmail.com)
*
*	Host Access	: https://minikube.sigs.k8s.io/docs/handbook/host-access/
*
 */

package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/georgelza/tmg_poster/database"
	"github.com/georgelza/tmg_poster/person"

	"google.golang.org/grpc"
	glog "google.golang.org/grpc/grpclog"
)

type tp_general struct {
	hostname   string
	debuglevel int
}

type tp_grpc struct {
	server string
	port   string
}

type tp_postgres struct {
	username string
	password string
	host     string
	port     int
	name     string
}

type tp_redis struct {
	username string
	password string
	host     string
	port     string
	db       int
}

type tp_mongo struct {
	username     string
	password     string
	host         string
	port         string
	database     string
	collection   string
	conn_timeout string
	uri_options  string
}

var (
	grpcLog glog.LoggerV2
)

func init() {

	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

	fmt.Println("")
	fmt.Println("###############################################################")
	fmt.Println("#")
	fmt.Println("#   File      : poster.go ")
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

}

func main() {

	grpcLog.Info("Retrieving variables ..")

	vGeneral := tp_general{hostname: ""}

	// Lets identify ourself
	vHostname, err := os.Hostname()
	if err != nil {
		grpcLog.Error("Can't retrieve hostname", err)
	}
	vGeneral.hostname = vHostname

	// Lets manage how much we prnt to the screen
	vGeneral.debuglevel, err = strconv.Atoi(os.Getenv("DEBUGLEVEL"))
	if err != nil {
		grpcLog.Error("String to Int convert error: %s", err)
	}

	vGRPC := tp_grpc{server: os.Getenv("GRPC_SERVER")}
	vGRPC.port = os.Getenv("GRPC_PORT")

	vPostgres := tp_postgres{username: os.Getenv("POSTGRES_USERNAME")}
	vPostgres.password = os.Getenv("POSTGRES_PASSWORD")
	vPostgres.host = os.Getenv("POSTGRES_HOST")
	vPostgres_port := os.Getenv("POSTGRES_PORT")
	vPostgres.name = os.Getenv("POSTGRES_NAME")

	vRedis := tp_redis{username: os.Getenv("REDIS_USERNAME")}
	vRedis.password = os.Getenv("REDIS_PASSWORD")
	vRedis.host = os.Getenv("REDIS_HOST")
	vRedis.port = os.Getenv("REDIS_PORT")
	vRedis_db := os.Getenv("REDIS_DB")

	vMongoDB := tp_mongo{username: os.Getenv("MONGODB_USERNAME")}
	vMongoDB.password = os.Getenv("MONGODB_PASSWORD")
	vMongoDB.host = os.Getenv("MONGODB_HOST")
	vMongoDB.port = os.Getenv("MONGODB_PORT")
	vMongoDB.database = os.Getenv("MONGODB_DATABASE")
	vMongoDB.collection = os.Getenv("MONGODB_COLLECTION")
	vMongoDB.conn_timeout = os.Getenv("MONGODB_CONN_TIMEOUT")
	vMongoDB.uri_options = os.Getenv("MONGODB_URI_OPTIONS")

	grpcLog.Info("****** General Parameters *****")
	grpcLog.Info("Hostname is\t\t\t", vGeneral.hostname)
	grpcLog.Info("Debug Level is\t\t", vGeneral.debuglevel)

	grpcLog.Info("****** gRPC Connection Parameters *****")
	grpcLog.Info("gRPC Server is\t\t", vGRPC.server)
	grpcLog.Info("gRPC Port is\t\t\t", vGRPC.port)

	grpcLog.Info("****** Postgres Connection Parameters *****")
	grpcLog.Info("Postgres DB Host is\t\t", vPostgres.host)
	grpcLog.Info("Postgres DB Port is\t\t", vPostgres_port)
	grpcLog.Info("Postgres DB Name is\t\t", vPostgres.name)
	grpcLog.Info("Postgres DB Username is\t", vPostgres.username)
	grpcLog.Info("Postgres DB Password is\t", vPostgres.password)

	grpcLog.Info("****** Redis Connection Parameters *****")
	grpcLog.Info("Redis DB Host is\t\t", vRedis.host)
	grpcLog.Info("Redis DB Port is\t\t", vRedis.port)
	grpcLog.Info("Redis DB Name is\t\t", vRedis_db)
	grpcLog.Info("Redis DB Username is\t\t", vRedis.username)
	grpcLog.Info("Redis DB Password is\t\t", vRedis.password)

	grpcLog.Info("****** MongoDB Connection Parameters *****")
	grpcLog.Info("MongoDB DB Host is\t\t", vMongoDB.host)
	grpcLog.Info("MongoDB DB Port is\t\t", vMongoDB.port)
	grpcLog.Info("MongoDB DB Database is\t\t", vMongoDB.database)
	grpcLog.Info("MongoDB DB Collection is\t\t", vMongoDB.collection)
	grpcLog.Info("MongoDB DB Username is\t\t", vMongoDB.username)
	grpcLog.Info("MongoDB DB Password is\t\t", vMongoDB.password)
	grpcLog.Info("MongoDB DB Conn Timeout is\t\t", vMongoDB.conn_timeout)
	grpcLog.Info("MongoDB DB URI Options is\t\t", vMongoDB.uri_options)

	grpcLog.Info("****** MariaDB Connection Parameters *****")

	vPostgres.port, err = strconv.Atoi(vPostgres_port)
	if err != nil {
		grpcLog.Error("Debug Level: String to Int convert error: %s", err)

	}

	vRedis.db, err = strconv.Atoi(vRedis_db)
	if err != nil {
		grpcLog.Error("Debug Level: String to Int convert error: %s", err)

	}

	//
	//
	grpcLog.Info("")
	grpcLog.Info("**** Configure gRPC Connection ****")

	// Setup gRPC Server environment
	listener, err := net.Listen("tcp", vGRPC.server+":"+vGRPC.port)
	if err != nil {
		grpcLog.Errorf("Failed to serve: %s", err)

	}
	grpcLog.Info("Created a listener on TCP port :", vGRPC.port)

	server := person.Server{}
	grpcLog.Info("Created a server instance")

	grpcServer := grpc.NewServer()
	grpcLog.Info("Created a gRPC server object")

	person.RegisterDataSaverServer(grpcServer, &server)
	grpcLog.Info("Attached the DataSaver service to the server")

	// Lets store some of these system wide values in my server object
	server.Debug_level = vGeneral.debuglevel

	// Lets identify ourself
	server.Hostname = vGeneral.hostname

	//
	//
	grpcLog.Info("")
	grpcLog.Info("**** Configure PostgreSQL Connection ****")

	// Setup PostgreSQL Database connect/Server environment
	server.Postgres_DBConn = database.Postgres_SetupDBConnection(
		vPostgres.name,
		vPostgres.username,
		vPostgres.password,
		vPostgres.port,
		vPostgres.host,
	)
	if server.Postgres_DBConn == nil {
		grpcLog.Errorf("Problem initialising Postgres database connection:")

	}
	defer server.Postgres_DBConn.Close()

	//
	//
	grpcLog.Info("")
	grpcLog.Info("**** Configure Redis Connection ****")

	// Setup Redis Database connect/Server environment
	server.Redis_DBConn = database.Redis_SetupDBConnection(
		vRedis.host,
		vRedis.port,
		vRedis.db,
		vRedis.username,
		vRedis.password,
	)
	if server.Redis_DBConn == nil {
		grpcLog.Errorf("Problem initialising Redis database connection:")

	}
	defer server.Redis_DBConn.Close()

	//
	//
	grpcLog.Info("")
	grpcLog.Info("**** Configure MongoDB Connection ****")

	// Setup Mongo Database connect/Server environment
	server.Mongo_DBConn = database.MongoDB_SetupDBConnection(
		vMongoDB.host,
		vMongoDB.port,
		vMongoDB.database,
		vMongoDB.collection,
		vMongoDB.username,
		vMongoDB.password,
		vMongoDB.conn_timeout,
		vMongoDB.uri_options,
	)
	if server.Mongo_DBConn == nil {
		grpcLog.Errorf("Problem initialising MongoDB connection:")

	}
	defer server.Mongo_DBConn.Disconnect()

	/*
	 *
	 *	OK, Lets Serve and Fly
	 *
	 */
	if err := grpcServer.Serve(listener); err != nil {
		grpcLog.Info("Failed to serve: %s", err)

	}

}
