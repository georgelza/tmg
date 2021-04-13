/*
*
*	File		: scrubber.go
*
* 	Created		: 30 Mar 2021
*
*	Description	: Here we subscribe to the Kafka Topc (poll and retrieve available msgs) and then push message the payloads to a
*				  gRPC Server implementation, we act as the gRPC Client.
*
*	Modified	: 31 Mar 2021
*				: 1 April 2021	- modified person.proto, structure was called Person, refactored to Message,
*								- added Body field, (remnant of the chat app).
*				: 4 April 2021 	- Add var grpcLog glog.LoggerV2 and associated init function, moved all log.FatalF -> grpcLog call
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
*
*
*	Look at rewriting consumer using code from: https://github.com/meitu/go-consumergroup/blob/master/example/example.go
*
*	How to Secure via TLS: See :
*	https://medium.com/pantomath/how-we-use-grpc-to-build-a-client-server-system-in-go-dd20045fa1c2
*
*	How to implement Logging via grpcLog :
*	https://github.com/tensor-programming/docker_grpc_chat_tutorial
*
 */

package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/georgelza/tmg_scrubber/person"
	"google.golang.org/grpc"
	glog "google.golang.org/grpc/grpclog"
	"google.golang.org/protobuf/proto"
)

var grpcLog glog.LoggerV2

func init() {
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)
}

func main() {

	dt := time.Now()
	fmt.Println("")
	fmt.Println("###############################################################")
	fmt.Println("#")
	fmt.Println("#   File      : scrubber.go ")
	fmt.Println("#")
	fmt.Println("#   By        : George Leonard (georgelza@gmail.com)")
	fmt.Println("#")
	fmt.Println("#   Date/Time :", dt.Format("02-01-2006 - 15:04:05"))
	fmt.Println("#")
	fmt.Println("###############################################################")
	fmt.Println("")

	// Lets manage how much we prnt to the screen
	var vDebugLevel, e1 = strconv.Atoi(os.Getenv("DEBUGLEVEL"))
	if e1 != nil {
		grpcLog.Error("String to Int convert error: %s", e1)

	}
	grpcLog.Info("Debug Level     : ", vDebugLevel)

	// Lets identify ourself
	var vHostname, e2 = os.Hostname()
	if e1 != nil {
		grpcLog.Error("Can't retrieve hostname", e2)
	}

	// gRPC Configuration
	var vGRPC_Server = os.Getenv("GRPC_SERVER")
	var vGRPC_Port = os.Getenv("GRPC_PORT")
	grpcLog.Info("gRPC Server     : ", vGRPC_Server)
	grpcLog.Info("gRPC Port       : ", vGRPC_Port)

	// Broker Configuration
	var vKafka_Broker = os.Getenv("KAFKA_BROKER")
	var vKafka_Port = os.Getenv("KAFKA_PORT")
	grpcLog.Info("Kafka Broker    : ", vKafka_Broker)
	grpcLog.Info("Kafka Port      : ", vKafka_Port)

	// --
	// The topic is passed as a pointer to the Consumer, so we can't
	// use a hard-coded literal. And a variable is a nicer way to do
	// it anyway ;-)
	var vTopic = os.Getenv("TOPIC")
	grpcLog.Info("Kafka Topic     : ", vTopic)

	fmt.Println("")

	// --
	// Create Consumer instance
	// https://rmoff.net/2020/07/14/learning-golang-some-rough-notes-s02e04-kafka-go-consumer-function-based/

	// Store the config
	cm := kafka.ConfigMap{
		"bootstrap.servers":    vKafka_Broker + ":" + vKafka_Port,
		"group.id":             "tmg_part2",
		"enable.partition.eof": true,
		"auto.offset.reset":    "latest"}

	// Variable p holds the new Consumer instance.
	c, e := kafka.NewConsumer(&cm)

	// Check for errors in creating the Consumer
	if e != nil {
		if ke, ok := e.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				grpcLog.Fatalf("😢 Can't create the Consumer because you've configured it wrong (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md", ec, e)

			default:
				grpcLog.Fatalf("😢 Can't create the Consumer (Kafka error code %d)\n\tError: %v\n", ec, e)

			}
		} else {
			// It's not a kafka.Error
			grpcLog.Fatalf("😢 Oh noes, there's a generic error creating the Consumer! %v", e.Error())

		}

	} else {

		// gRPC object creation
		var conn *grpc.ClientConn
		conn, err := grpc.Dial(vGRPC_Server+":"+vGRPC_Port, grpc.WithInsecure())
		if err != nil {
			grpcLog.Fatalf("gRPC client connection failed: %s", err)

		}
		defer conn.Close()

		cgRPC := person.NewDataSaverClient(conn)

		// Subscribe to the topic
		if e := c.Subscribe(vTopic, nil); e != nil {
			grpcLog.Fatalf("☠️ Uh oh, there was an error subscribing to the topic :\n\t%v\n", e)

		} else {
			grpcLog.Info("gRPC client connection established: \n\n")

			run := true
			for run == true {
				ev := c.Poll(0)

				switch e := ev.(type) {
				case *kafka.Message:
					if vDebugLevel > 1 {
						grpcLog.Infof("\n%% Message on %s: %s", e.TopicPartition, e.Value, "\n\n")

					}

					// Unmarshall e.Value into message
					message := &person.Message{}
					err := proto.Unmarshal(e.Value, message)
					if err != nil {
						grpcLog.Fatal("unmarshaling error: ", err)

					}

					message.Path += ",Scrubber:[" + vHostname + "," + time.Now().Format("02-01-2006 - 15:04:05.0000") + "]"

					// Marshal message into serializedPerson
					serializedPerson, err := proto.Marshal(message)
					if vDebugLevel == 1 {
						fmt.Print(serializedPerson)
					}

					// Posting to the gRPC end point living on the server
					response, err := cgRPC.PostData(context.Background(), message)
					if err != nil {
						//fmt.Print("Error when calling PostData: ", err)
						//fmt.Print("Error when calling PostData: %s", err)
						//log.Fatalf("Error when calling PostData: %s", err)
						grpcLog.Errorf("Error when calling PostData: %s", err)

					}
					grpcLog.Infof("Response from server: %s ", fmt.Sprintf("%s: %s", response.Uuid, response.Note))

				case kafka.PartitionEOF:
					grpcLog.Infof("Reached %v\n", e)

				case kafka.Error:
					grpcLog.Errorf("Error: %v\n", e)

					run = false

				}
			}
			c.Close()

		}
	}

}
