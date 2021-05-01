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
*	See https://www.youtube.com/watch?v=p45_9nOpD4k&t=911s - implementing logrus, with json, -> NICE
*		https://github.com/sirupsen/logrus
*
*	https://github.com/abhirockzz/kafka-go-docker-quickstart
*	https://github.com/confluentinc/confluent-kafka-go/issues/461
*
*
*	Client Listener Management for Kafka :
*	https://www.confluent.io/blog/kafka-client-cannot-connect-to-broker-on-aws-on-docker-etc/?_ga=2.11602868.1661736099.1618811110-1053893990.1617181319&_gac=1.48689876.1618906682.Cj0KCQjw9_mDBhCGARIsAN3PaFOC1huRcGJmzf2JImKck4ykajJ_IvDMahxucfMuq-PTI5N4qQYdkOUaAhM4EALw_wcB
*	https://rmoff.net/2020/07/17/learning-golang-some-rough-notes-s02e08-checking-kafka-advertised.listeners-with-go/
*	https*
*
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

type tp_general struct {
	hostname   string
	debuglevel int
}

type tp_grpc struct {
	server string
	port   string
}

type tp_kafka struct {
	broker            string
	port              string
	topic             string
	numpartitions     int
	replicationfactor int
	retension         string
	consumergroupid   string
}

var (
	grpcLog glog.LoggerV2
)

func init() {
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

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
	vDebugLevel, err := strconv.Atoi(os.Getenv("DEBUGLEVEL"))
	if err != nil {
		grpcLog.Error("String to Int convert error: %s", err)
	}
	vGeneral.debuglevel = vDebugLevel

	vGRPC := tp_grpc{server: os.Getenv("GRPC_SERVER")}
	vGRPC.port = os.Getenv("GRPC_PORT")

	// Broker Configuration
	vKafka := tp_kafka{broker: os.Getenv("KAFKA_BROKER")}
	vKafka.port = os.Getenv("KAFKA_PORT")
	vKafka.topic = os.Getenv("KAFKA_TOPIC")
	vKafka_NumPartitions := os.Getenv("KAFKA_NUMPARTITIONS")
	vKafka_ReplicationFactor := os.Getenv("KAFKA_REPLICATIONFACTOR")
	vKafka.retension = os.Getenv("KAFKA_RETENSION")
	vKafka.consumergroupid = os.Getenv("KAFKA_CONSUMERGROUPID")

	grpcLog.Info("****** General Parameters *****")
	grpcLog.Info("Hostname is\t\t", vGeneral.hostname)
	grpcLog.Info("Debug Level is\t", vGeneral.debuglevel)

	grpcLog.Info("****** gRPC Connection Parameters *****")
	grpcLog.Info("gRPC Server is\t", vGRPC.server)
	grpcLog.Info("gRPC Port is\t\t", vGRPC.port)

	grpcLog.Info("****** Kafka Connection Parameters *****")
	grpcLog.Info("Kafka Broker is\t\t", vKafka.broker)
	grpcLog.Info("Kafka Port is\t\t\t", vKafka.port)
	grpcLog.Info("Kafka Topic is\t\t", vKafka.topic)
	grpcLog.Info("Kafka # Parts is\t\t", vKafka_NumPartitions)
	grpcLog.Info("Kafka Rep Factor is\t\t", vKafka_ReplicationFactor)
	grpcLog.Info("Kafka Retension is\t\t", vKafka.retension)
	grpcLog.Info("Kafka Group is\t\t", vKafka.consumergroupid)

	// Lets manage how much we prnt to the screen
	vKafka.numpartitions, err = strconv.Atoi(vKafka_NumPartitions)
	if err != nil {
		grpcLog.Error("vKafka_NumPartitions, String to Int convert error: %s", err)

	}

	// Lets manage how much we prnt to the screen
	vKafka.replicationfactor, err = strconv.Atoi(vKafka_ReplicationFactor)
	if err != nil {
		grpcLog.Error("vKafka_ReplicationFactor, String to Int convert error: %s", err)

	}

	grpcLog.Info("")
	grpcLog.Info("**** Configure Admin Kafka Connection ****")

	// --
	// Create Consumer instance
	// https://rmoff.net/2020/07/14/learning-golang-some-rough-notes-s02e04-kafka-go-consumer-function-based/

	acm_str := fmt.Sprintf("%s:%s", vKafka.broker, vKafka.port)
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
	tcm["retention.ms"] = vKafka.retension // Default 604 800 000 => 7 days, 36 00 000 => 1 hour
	grpcLog.Info("retention.ms Configured")

	results, err := a.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:             vKafka.topic,
			NumPartitions:     vKafka.numpartitions,
			ReplicationFactor: vKafka.replicationfactor,
			Config:            tcm}},

		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))

	if err != nil {
		grpcLog.Errorf("Failed to create topic: %v\n", err)
		os.Exit(1)
	}
	grpcLog.Infof("Topic %s Created\n", vKafka.topic)

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
		"group.id":             vKafka.consumergroupid,
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
		os.Exit(1)

	} else {
		grpcLog.Info("Created Kafka Consumer instance :")
		grpcLog.Info("")

		grpcLog.Info("**** Configure gRPC Connection ****")

		dial_str := fmt.Sprintf("%s:%s", vGRPC.server, vGRPC.port)
		grpcLog.Info("dial_str is\t\t", dial_str)

		// gRPC object creation
		var conn *grpc.ClientConn
		conn, err := grpc.Dial(dial_str, grpc.WithInsecure())
		if err != nil {
			grpcLog.Fatalf("gRPC client connection failed: %s", err)
			os.Exit(1)

		}
		grpcLog.Info("gRPC Dialed Successfully :")
		grpcLog.Info("gRPC Client connection established: \n")

		defer conn.Close()

		cgRPC := person.NewDataSaverClient(conn)
		grpcLog.Info("gRPC Protobuf DataSaverServer Object created :")

		// Subscribe to the topic
		if e := c.Subscribe(vKafka.topic, nil); e != nil {
			grpcLog.Fatalf("☠️ Uh oh, there was an error subscribing to the topic :\n\t%v\n", e)
			os.Exit(1)

		} else {
			grpcLog.Info("Subscribed to Kafka Topic: ", vKafka.topic)
			grpcLog.Info("")

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

					message.Path += ",Scrubber:[" + vGeneral.hostname + "," + time.Now().Format("02-01-2006 - 15:04:05.0000") + "]"

					if vGeneral.debuglevel == 2 {
						grpcLog.Infof("\n%% Hostname: %s Recieved Message on %s: %s: %s", vGeneral.hostname, e.TopicPartition, message.Seq, message.Uuid, "\n")

					} else if vGeneral.debuglevel == 3 {
						grpcLog.Infof("\n%% Hostname: %s Recieved Message on %s: %s", vGeneral.hostname, e.TopicPartition, e.Value, "\n")

					} else if vGeneral.debuglevel == 4 {
						// Marshal message into serializedPerson
						serializedPerson, err := proto.Marshal(message)
						if err != nil {
							fmt.Print("Hostname: ", vGeneral.hostname, serializedPerson)

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