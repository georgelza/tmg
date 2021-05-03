/*
*
*	File		: filegester.go
*
* 	Created		: 30 Mar 2021
*
*	Description	: Here we read a text file, line by line and publish onto a Kafka topic, message payload
*				  serialized using Protobuf structure
*
*	Modified	: 31 Mar 2021	- Start
*				: 1 April 2021	- modified person.proto, structure was called Person, refactored to Message,
*								- added Body field, (remnant of the chat app).
*				: 28 April 2021 - Added dest to the protobuf message, to be used to determine the destination database
*				: 1 May 2021	- Moved general and kafka input params to a variable based on a struct
*				: 2 May 2021 	- Added prompt at beginning to get the dest database specified
*
*	By			: George Leonard (georgelza@gmail.com)
*
*
*  	This is part #1 of a little project to teach myself golang and various other technologies
*	Included will be Protobufs and gRPC,
*	Kafka
*	ProgreSQL - working
*	MySQL - to be done
*	CockroachDB - to be done
*	Cassandra - not liking, causing loads of instability, 2Gb Java footprint
*	Prometheus and Grafana - to be done
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
*	Lets add Schema Registry: https://github.com/riferrei/srclient/issues/17
*
 */

package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	glog "google.golang.org/grpc/grpclog"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/georgelza/tmg_filegester/person"
	guuid "github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type tp_general struct {
	hostname   string
	debuglevel int
	testsize   int
	dest       string
	filedir    string
	datafile   string
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
	m       = make(map[string]string)
)

func init() {
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)

	fmt.Println("###############################################################")
	fmt.Println("#")
	fmt.Println("#   File      : Filegester ")
	fmt.Println("#")
	fmt.Println("#	Comment		: Reads fake input data from text file and publishes")
	fmt.Println("#		        : onto Kafka topic")
	fmt.Println("#")
	fmt.Println("#   By        : George Leonard (georgelza@gmail.com)")
	fmt.Println("#")
	fmt.Println("#   Date/Time :", time.Now().Format("02-01-2006 - 15:04:05"))
	fmt.Println("#")
	fmt.Println("###############################################################")
	fmt.Println("")
	fmt.Println(" 	Please select destination database")
	fmt.Println("*")
	fmt.Println("* 1 - PostgreSQL")
	fmt.Println("* 2 - Redis")
	fmt.Println("* 3 - MongoDB")
	fmt.Println("* 4 - MariaDB")
	fmt.Println("*")

}

func main() {

	m["1"] = "postgres"
	m["2"] = "redis"
	m["3"] = "mongodb"
	m["4"] = "mariadb"

	fmt.Println("insert y value here: ")
	input := bufio.NewScanner(os.Stdin)
	input.Scan()

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

	vGeneral.testsize, err = strconv.Atoi(os.Getenv("TESTSIZE"))
	if err != nil {
		grpcLog.Error("String to Int convert error: %s", err)
	}

	vGeneral.dest = m[input.Text()]

	// File and directory
	vGeneral.filedir = os.Getenv("FILEDIR")
	vGeneral.datafile = os.Getenv("DATAFILE")

	// Broker Configuration
	vKafka := tp_kafka{broker: os.Getenv("KAFKA_BROKER")}
	vKafka.port = os.Getenv("KAFKA_PORT")
	vKafka.topic = os.Getenv("KAFKA_TOPIC")
	vKafka_NumPartitions := os.Getenv("KAFKA_NUMPARTITIONS")
	vKafka_ReplicationFactor := os.Getenv("KAFKA_REPLICATIONFACTOR")
	vKafka.retension = os.Getenv("KAFKA_RETENSION")
	vKafka.consumergroupid = os.Getenv("KAFKA_CONSUMERGROUPID")

	grpcLog.Info("****** General Parameters *****")
	grpcLog.Info("Hostname is\t\t\t", vGeneral.hostname)
	grpcLog.Info("Debug Level is\t\t", vGeneral.debuglevel)
	grpcLog.Info("Destination Database is\t", vGeneral.dest)
	grpcLog.Info("File Directory is\t\t", vGeneral.filedir)
	grpcLog.Info("Data File is\t\t\t", vGeneral.datafile)
	grpcLog.Info("Test Size is\t\t\t", vGeneral.testsize)

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

	// Create admin client to create the topic if it does not exist
	grpcLog.Info("")
	grpcLog.Info("**** Configure Admin Kafka Connection ****")

	acm_str := fmt.Sprintf("%s:%s", vKafka.broker, vKafka.port)
	grpcLog.Info("acm_str is\t\t\t", acm_str)

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

	// --
	// Create Producer instance
	// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#NewProducer

	grpcLog.Info("")
	grpcLog.Info("**** Configure Client Kafka Connection ****")

	cm_str := fmt.Sprintf("%s:%s", vKafka.broker, vKafka.port)
	grpcLog.Info("cm_str is\t\t\t", cm_str)
	grpcLog.Info("client.id is\t\t\t", vGeneral.hostname)

	// Store the client config
	cm := kafka.ConfigMap{"bootstrap.servers": acm_str,
		"client.id": vGeneral.hostname}

	// Variable p holds the new Producer instance.
	p, err := kafka.NewProducer(&cm)

	// Check for errors in creating the Producer
	if err != nil {
		grpcLog.Errorf("😢Oh noes, there's an error creating the Producer! ", err)

		if ke, ok := err.(kafka.Error); ok == true {
			switch ec := ke.Code(); ec {
			case kafka.ErrInvalidArg:
				grpcLog.Fatalf("😢 Can't create the producer because you've configured it wrong (code: %d)!\n\t%v\n\nTo see the configuration options, refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md", ec, err)

			default:
				grpcLog.Fatalf("😢 Can't create the producer (Kafka error code %d)\n\tError: %v\n", ec, err)
			}

		} else {
			// It's not a kafka.Error
			grpcLog.Fatalf("😢 Oh noes, there's a generic error creating the Producer! %v", err.Error())

		}
		// call it when you know it's broken
		os.Exit(1)

	} else {
		grpcLog.Info("Created Kafka Producer instance :")
		grpcLog.Info("")

		///////////////////////////////////////////////////////
		//
		// Successful connection established with Kafka Cluster
		//
		///////////////////////////////////////////////////////

		// For signalling termination from main to go-routine
		termChan := make(chan bool, 1)
		// For signalling that termination is done from go-routine to main
		doneChan := make(chan bool)

		// Build a fully qualified path to the data file
		vFile := filepath.Join(vGeneral.filedir, vGeneral.datafile)

		// Open my source file
		f_InputData, err := os.Open(vFile)
		if err != nil {
			grpcLog.Errorf("Error happened while processing", err)

		}
		defer f_InputData.Close()
		grpcLog.Info("Source Data File opened :")

		// Lets loop through the file, reading it line by line
		scanner := bufio.NewScanner(f_InputData)
		count := 0
		for scanner.Scan() {

			// we want to skip the first line as it's a header line
			if count != 0 {

				// Split the file based on comman's into a array
				s := strings.Split(scanner.Text(), ",")
				message := &person.Message{
					Dest:      vGeneral.dest,
					Uuid:      guuid.New().String(),
					Path:      "FileGester:[" + vGeneral.hostname + "," + time.Now().Format("02-01-2006 - 15:04:05.0000") + "]",
					Seq:       s[0],
					Alpha:     s[1],
					First:     s[2],
					Last:      s[3],
					Birthday:  s[4],
					Gender:    s[5],
					Email:     s[6],
					Street:    s[7],
					State:     s[8],
					City:      s[9],
					Zip:       s[10],
					Ccnumber:  s[11],
					Date:      s[12],
					Latitude:  s[13],
					Longitude: s[14],
					Dollar:    s[15],
					Note:      "",
				}

				serializedPerson, err := proto.Marshal(message)
				if err != nil {
					grpcLog.Errorf("Marchalling error: ", err)

				}

				// --
				// Send a message using Produce()
				// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#Producer.Produce
				//
				// Build the message objects
				// We're going to key the data on "state" column, this will help down the line in the project during consumptions.
				kafkaMsg := kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &vKafka.topic, Partition: kafka.PartitionAny},
					Value:          serializedPerson,
					Key:            []byte(s[8])}

				// We will decide if we want to keep this bit!!! or simplify it.
				//
				// Handle any events that we get
				go func() {
					doTerm := false
					for !doTerm {
						// The `select` blocks until one of the `case` conditions
						// are met - therefore we run it in a Go Routine.
						select {
						case ev := <-p.Events():
							// Look at the type of Event we've received
							switch ev.(type) {

							case *kafka.Message:
								// It's a delivery report
								km := ev.(*kafka.Message)
								if km.TopicPartition.Error != nil {
									if vGeneral.debuglevel > 2 {
										fmt.Printf("☠️ Failed to send message '%v' to topic '%v'\n\tErr: %v",
											string(km.Value),
											string(*km.TopicPartition.Topic),
											km.TopicPartition.Error)

									}

								} else {
									if vGeneral.debuglevel == 1 {
										fmt.Println(message.Seq, " ", message.First, " ", message.Last)

									} else if vGeneral.debuglevel == 2 {

										// for now we just dump the data to a text file, to be replaced with the publish to a Kafka topic
										fmt.Println(message.Seq, " ", message.Uuid, " ", message.First, " ", message.Last)

									} else if vGeneral.debuglevel == 3 {

										// for now we just dump the data to a text file, to be replaced with the publish to a Kafka topic
										fmt.Println(message.Seq, " ", message.First, " ", message.Last)
										fmt.Println(serializedPerson)
										fmt.Println("")

									} else if vGeneral.debuglevel == 4 {

										fmt.Printf("✅ Message '%v' delivered to topic '%v'(partition %d at offset %d)\n",
											string(km.Value),
											string(*km.TopicPartition.Topic),
											km.TopicPartition.Partition,
											km.TopicPartition.Offset)
									}
								}

							case kafka.Error:
								// It's an error
								em := ev.(kafka.Error)
								grpcLog.Errorf("☠️ Uh oh, caught an error:\n\t%v\n", em)

								//default:
								// It's not anything we were expecting
								//		fmt.Printf("Got an event that's not a Message or Error 👻\n\t%v\n", ev)

							}
						case <-termChan:
							doTerm = true

						}
					}
					close(doneChan)
				}()

				// This is where we publish message onto the topic... on the cluster
				// Produce the message
				if e := p.Produce(&kafkaMsg, nil); e != nil {
					grpcLog.Fatalf("😢 Darn, there's an error producing the message!", e.Error())

				}

				if count == vGeneral.testsize {
					break
				}
			}
			count++

		}

		// --
		// Flush the Producer queue
		t := 10000
		if r := p.Flush(t); r > 0 {
			grpcLog.Errorf("\n--\n⚠️ Failed to flush all messages after %d milliseconds. %d message(s) remain\n", t, r)

		} else {
			grpcLog.Infoln("\n--\n✨ All messages flushed from the queue")

		}
		// --
		// Stop listening to events and close the producer
		// We're ready to finish
		termChan <- true

		// wait for go-routine to terminate
		<-doneChan
		defer p.Close()

	}
}
