/*
*
*	File		: server.go
*
* 	Created		: 1 Apr 2021
*
*	Description	: ??? - Lets add something here
*
*	Modified	: 1 April 2021	Started
*				: 4 April 2021 	Add var grpcLog glog.LoggerV2 and associated init function, moved all log.FatalF -> grpcLog call, and then removed some of this again.
*				: 5 April 2021	Introduced config.yml for now for configuration values, as I need to internalize it... playing with docker,
*				:    			Will go back to env variables when we deploy into k8s
*
*	By			: George Leonard (georgelza@gmail.com)
*
*
*  	This is part #1 of a little project to teach myself golang and various other technologies
*	Included will be Protobufs and gRPC,
*	Kafka
*	ProgreSQL
*	MySQL
*	CockroachDB
*	Cassandra
*	Prometheus and Grafana
*
*
* 	All to be deployed onto docker - multiple consumers from cluster/topic storing into various end state databases
*
*
* 	go get gopkg.in/confluentinc/confluent-kafka-go.v1/kafka
*
*
* 	https://docs.confluent.io/platform/current/app-development/kafkacat-usage.html
* 	kafkacat -b localhost:9092 -t people_pb
*
*	Bring Confluent Cluster up	: docker-compose up -d
*	Status of cluster 			: docker-compose ps
*	Take down cluster			: docker-compuse down
*
*	https://github.com/confluentinc/confluent-kafka-go
*
*	How to read yml file, nice easy tutorial.
*	https://medium.com/@bnprashanth256/reading-configuration-files-and-environment-variables-in-go-golang-c2607f912b63
*
*	How to Secure via TLS: See :
*	https://medium.com/pantomath/how-we-use-grpc-to-build-a-client-server-system-in-go-dd20045fa1c2
*
*	How to implement Logging via grpcLog :
*	https://github.com/tensor-programming/docker_grpc_chat_tutorial
*	https://www.honeybadger.io/blog/golang-logging/
*	https://golang.org/pkg/log/
*	https://www.datadoghq.com/blog/go-logging/
*	https://medium.com/@pradityadhitama/simple-logger-in-golang-f72dadf2c8c6
*
*
*
 */

package main

import (
	"fmt"
	"net"
	"os"
	"time"

	"github.com/georgelza/tmg_poster_term/person"
	"google.golang.org/grpc"
	glog "google.golang.org/grpc/grpclog"

	"github.com/georgelza/tmg_poster_term/config"
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
	fmt.Println("#   File      : poster_server.go ")
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
	viper.SetDefault("database.dbname", "test_db")

	err := viper.Unmarshal(&configuration)
	if err != nil {
		grpcLog.Errorf("Unable to decode into struct, %v", err)
	}

	// Reading variables using the model
	grpcLog.Info("Reading variables using the model..")
	grpcLog.Info("gRPC Port is\t\t", configuration.GRPC_Server.Port)
	grpcLog.Info("gRPC Name is\t\t", configuration.GRPC_Server.Name)
	grpcLog.Info("DEBUGLEVEL is\t\t", configuration.DEBUGLEVEL)

	grpcLog.Info("")

	listener, err := net.Listen("tcp", configuration.GRPC_Server.Name+":"+configuration.GRPC_Server.Port)
	if err != nil {
		grpcLog.Errorf("Failed to serve: %s", err)

	}

	grpcLog.Info("create a listener on TCP port :", configuration.GRPC_Server.Port)

	server := person.Server{}
	grpcLog.Info("create a server instance")

	grpcServer := grpc.NewServer()
	grpcLog.Info("create a gRPC server object")

	person.RegisterDataSaverServer(grpcServer, &server)
	grpcLog.Info("attach the DataSaver service to the server")

	if err := grpcServer.Serve(listener); err != nil {
		grpcLog.Info("Failed to serve: %s", err)

	}
}
