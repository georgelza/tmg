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
*				: 14 April 2021	- Introduce the config.yml file for all variables
*
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
*	https://github.com/abhirockzz/kafka-go-docker-quickstart
*	https://github.com/confluentinc/confluent-kafka-go/issues/461
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

	fmt.Println("")
	fmt.Println("###############################################################")
	fmt.Println("#")
	fmt.Println("#	File      	: scrubber.go ")
	fmt.Println("#")
	fmt.Println("#	Comment		: Consumes msgs from Kafka Cluster/topic & sends as a")
	fmt.Println("#		        : gRPC/Protobuf msg to Poster* app that saves payload into DB")
	fmt.Println("#")
	fmt.Println("#	By    		: George Leonard (georgelza@gmail.com)")
	fmt.Println("#")
	fmt.Println("#	Date/Time	:", time.Now().Format("02-01-2006 - 15:04:05"))
	fmt.Println("#")
	fmt.Println("###############################################################")
	fmt.Println("")

	// Lets identify ourself
	var vHostname, e1 = os.Hostname()
	if e1 != nil {
		grpcLog.Error("Can't retrieve hostname", e1)
	}

	// Reading variables using the model
	grpcLog.Info("Retrieving variables ..")

	var vGRPC_Server = os.Getenv("GRPC_SERVER")
	var vGRPC_Port = os.Getenv("GRPC_PORT")

	var vKafka_Broker = os.Getenv("KAFKA_BROKER")
	var vKafka_Port = os.Getenv("KAFKA_PORT")
	var vKafka_Topic = os.Getenv("KAFKA_TOPIC")
	var vKafka_NumPartitions = os.Getenv("KAFKA_NUMPARTITIONS")
	var vKafka_ReplicationFactor = os.Getenv("KAFKA_REPLICATIONFACTOR")
	var vKafka_Retension = os.Getenv("KAFKA_RETENSION")
	var vKafka_ConsumerGroupID = os.Getenv("KAFKA_CONSUMERGROUPID")

	var vDebug_Level = os.Getenv("DEBUGLEVEL")

	grpcLog.Info("Hostname is\t\t", vHostname)

	grpcLog.Info("gRPC Server is\t", vGRPC_Server)
	grpcLog.Info("gRPC Port is\t\t", vGRPC_Port)

	grpcLog.Info("Kafka Broker is\t", vKafka_Broker)
	grpcLog.Info("Kafka Port is\t\t", vKafka_Port)
	grpcLog.Info("Kafka Topic is\t", vKafka_Topic)
	grpcLog.Info("Kafka # Parts is\t", vKafka_NumPartitions)
	grpcLog.Info("Kafka Rep Factor is\t", vKafka_ReplicationFactor)
	grpcLog.Info("Kafka Retension is\t", vKafka_Retension)
	grpcLog.Info("Kafka Group is\t", vKafka_ConsumerGroupID)

	grpcLog.Info("Debug Level is\t", vDebug_Level)

	// Lets manage how much we prnt to the screen
	var vDebugLevel, e2 = strconv.Atoi(vDebug_Level)
	if e2 != nil {
		grpcLog.Error("vDebugLevel, String to Int convert error: %s", e2)

	}

	// Lets manage how much we prnt to the screen
	var vKafka_Num_Partitions, e3 = strconv.Atoi(vKafka_NumPartitions)
	if e3 != nil {
		grpcLog.Error("vKafka_NumPartitions, String to Int convert error: %s", e3)

	}

	// Lets manage how much we prnt to the screen
	var vKafka_Replication_Factor, e4 = strconv.Atoi(vKafka_ReplicationFactor)
	if e4 != nil {
		grpcLog.Error("vKafka_ReplicationFactor, String to Int convert error: %s", e4)

	}

	grpcLog.Info("")
	grpcLog.Info("**** Configure Admin Kafka Connection ****")

	// --
	// Create Consumer instance
	// https://rmoff.net/2020/07/14/learning-golang-some-rough-notes-s02e04-kafka-go-consumer-function-based/

	acm_str := fmt.Sprintf("%s:%s", vKafka_Broker, vKafka_Port)
	grpcLog.Info("acm_str is\t\t", acm_str)

	// Store the Admin config
	acm := kafka.ConfigMap{"bootstrap.servers": acm_str}

	// Create a new AdminClient.
	// AdminClient can also be instantiated using an existing
	// Producer or Consumer instance, see NewAdminClientFromProducer and
	// NewAdminClientFromConsumer.
	a, err := kafka.NewAdminClient(&acm)
	if err != nil {
		grpcLog.Errorf("Failed to create Admin client: %s\n", err)
		os.Exit(1)

	}
	grpcLog.Info("Admin client created")

	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grpcLog.Info("Context object created")

	// Create topics on cluster.
	// Set Admin options to wait for the operation to finish (or at most 60s)
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		grpcLog.Errorf("ParseDuration Exceeded %v\n", err)
		panic("ParseDuration(60s)")

	}
	grpcLog.Info("ParseDuration Configured")

	var tcm = make(map[string]string)
	tcm["retention.ms"] = vKafka_Retension // Default 604 800 000 => 7 days, 36 00 000 => 1 hour
	grpcLog.Info("retention.ms Configured")

	results, err := a.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:             vKafka_Topic,
			NumPartitions:     vKafka_Num_Partitions,
			ReplicationFactor: vKafka_Replication_Factor,
			Config:            tcm}},

		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))

	if err != nil {
		grpcLog.Errorf("Failed to create topic: %v\n", err)
		os.Exit(1)
	}
	grpcLog.Infof("Topic %s Created\n", vKafka_Topic)

	// Print results
	for _, result := range results {
		grpcLog.Infof("%s\n", result)

	}

	a.Close()

	grpcLog.Info("")
	grpcLog.Info("**** Configure Client Kafka Connection ****")

	// Lets configure the Kafka Client Connection now
	// Store the Client config
	cm := kafka.ConfigMap{
		"bootstrap.servers":    acm_str,
		"group.id":             vKafka_ConsumerGroupID,
		"enable.partition.eof": true,
		"auto.offset.reset":    "latest"}

	// Variable p holds the new Consumer instance.
	c, e := kafka.NewConsumer(&cm)
	grpcLog.Info("Created Kafka Consumer instance :")
	grpcLog.Info("")

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
		os.Exit(1)

	} else {

		grpcLog.Info("**** Configure gRPC Connection ****")

		// gRPC object creation
		var conn *grpc.ClientConn
		conn, err := grpc.Dial(vGRPC_Server+":"+vGRPC_Port, grpc.WithInsecure())
		if err != nil {
			grpcLog.Fatalf("gRPC client connection failed: %s", err)
			os.Exit(1)

		}
		grpcLog.Info("gRPC Dialed :")

		defer conn.Close()

		cgRPC := person.NewDataSaverClient(conn)
		grpcLog.Info("gRPC Protobuf DataSaverServer Object created :")

		// Subscribe to the topic
		if e := c.Subscribe(vKafka_Topic, nil); e != nil {
			grpcLog.Fatalf("☠️ Uh oh, there was an error subscribing to the topic :\n\t%v\n", e)
			os.Exit(1)

		} else {
			grpcLog.Info("gRPC client connection established: \n\n")

			run := true
			for run == true {
				ev := c.Poll(0)

				switch e := ev.(type) {
				case *kafka.Message:

					// Unmarshall e.Value into message
					message := &person.Message{}
					err := proto.Unmarshal(e.Value, message)
					if err != nil {
						grpcLog.Fatal("unmarshaling error: ", err)

					}

					message.Path += ",Scrubber:[" + vHostname + "," + time.Now().Format("02-01-2006 - 15:04:05.0000") + "]"

					if vDebugLevel == 2 {
						grpcLog.Infof("\n%% Hostname: %s Recieved Message on %s: %s: %s", vHostname, e.TopicPartition, message.Seq, message.Uuid, "\n")

					} else if vDebugLevel == 3 {
						grpcLog.Infof("\n%% Hostname: %s Recieved Message on %s: %s", vHostname, e.TopicPartition, e.Value, "\n")

					} else if vDebugLevel == 4 {
						// Marshal message into serializedPerson
						serializedPerson, err := proto.Marshal(message)
						if err != nil {
							fmt.Print("Hostname: ", vHostname, serializedPerson)

						}
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
