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
*	Lets add Schema Registry: https://github.com/riferrei/srclient/issues/17
*
 */

package main

import (
	"bufio"
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

var grpcLog glog.LoggerV2

func init() {
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)
}

func main() {

	dt := time.Now()
	fmt.Println("###############################################################")
	fmt.Println("#")
	fmt.Println("#   File      : Filegester ")
	fmt.Println("#")
	fmt.Println("#   By        : George Leonard (georgelza@gmail.com)")
	fmt.Println("#")
	fmt.Println("#   Date/Time :", dt.Format("02-01-2006 - 15:04:05"))
	fmt.Println("#")
	fmt.Println("###############################################################")
	fmt.Println("")

	var vTestSize, e1 = strconv.Atoi(os.Getenv("TESTSIZE"))
	if e1 != nil {
		grpcLog.Error("String to Int convert error: %s", e1)
	}
	grpcLog.Info("Test Size       : ", vTestSize)

	// Lets manage how much we prnt to the screen
	var vDebugLevel, e2 = strconv.Atoi(os.Getenv("DEBUGLEVEL"))
	if e2 != nil {
		grpcLog.Error("String to Int convert error: %s", e2)
	}
	grpcLog.Info("Debug Level     : ", vDebugLevel)

	// Lets identify ourself
	var vHostname, e3 = os.Hostname()
	if e3 != nil {
		grpcLog.Error("Can't retrieve hostname", e3)
	}

	// File and directory
	var vFileDir = os.Getenv("FILEDIR")
	var vDataFile = os.Getenv("DATAFILE")
	grpcLog.Info("FILEDIR         : ", vFileDir)
	grpcLog.Info("DATAFILE        : ", vDataFile)

	// Broker Configuration
	var vKafka_Broker = os.Getenv("KAFKA_BROKER")
	var vKafka_Port = os.Getenv("KAFKA_PORT")
	grpcLog.Info("Kafka Broker    : ", vKafka_Broker)
	grpcLog.Info("Kafka Port      : ", vKafka_Port)

	// --
	// The topic is passed as a pointer to the Producer, so we can't
	// use a hard-coded literal. And a variable is a nicer way to do
	// it anyway ;-)
	var vTopic = os.Getenv("TOPIC")
	grpcLog.Info("Kafka Topic     : ", vTopic)

	fmt.Println("")

	// --
	// Create Producer instance
	// https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#NewProducer

	// Store the config
	c := kafka.ConfigMap{
		"bootstrap.servers": vKafka_Broker + ":" + vKafka_Port,
		"client.id":         vHostname}

	// Variable p holds the new Producer instance.
	p, err := kafka.NewProducer(&c)

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
		vFile := filepath.Join(vFileDir, vDataFile)

		// Open my source file
		f_InputData, err := os.Open(vFile)
		if err != nil {
			grpcLog.Errorf("Error happened while processing", err)

		}
		defer f_InputData.Close()

		// Lets loop through the file, reading it line by line
		scanner := bufio.NewScanner(f_InputData)
		count := 0
		for scanner.Scan() {

			// we want to skip the first line as it's a header line
			if count != 0 {

				// Split the file based on comman's into a array
				s := strings.Split(scanner.Text(), ",")
				message := &person.Message{
					Uuid:      guuid.New().String(),
					Path:      "FileGester:[" + vHostname + "," + time.Now().Format("02-01-2006 - 15:04:05.0000") + "]",
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
					TopicPartition: kafka.TopicPartition{Topic: &vTopic, Partition: kafka.PartitionAny},
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
									if vDebugLevel > 2 {
										fmt.Printf("☠️ Failed to send message '%v' to topic '%v'\n\tErr: %v",
											string(km.Value),
											string(*km.TopicPartition.Topic),
											km.TopicPartition.Error)

									}

								} else {
									if vDebugLevel > 1 {

										fmt.Printf("✅ Message '%v' delivered to topic '%v' (partition %d at offset %d)\n",
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

				if vDebugLevel == 1 {
					fmt.Println(serializedPerson)

				} else {
					if vDebugLevel > 1 {
						// for now we just dump the data to a text file, to be replaced with the publish to a Kafka topic
						fmt.Println(message.Seq, " ", message.First, " ", message.Last)
						fmt.Println(serializedPerson)
						fmt.Println("")
					}
				}

				if count == vTestSize {
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
