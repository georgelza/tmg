package person

import (
	"encoding/json"
	"os"
	"time"

	"github.com/georgelza/tmg_poster/database"

	"golang.org/x/net/context"
	glog "google.golang.org/grpc/grpclog"
	"google.golang.org/protobuf/encoding/protojson"
)

var grpcLog glog.LoggerV2

func init() {
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)
}

type Server struct {
	Postgres_dbConn *database.Postgres_dbConnection
	Redis_dbConn    *database.Redis_dbConnection
	Mongo_dbConn    *database.Mongo_dbConnection
	Maria_dbConn    *database.Maria_dbConnection

	Debug_level int
	Hostname    string
}

func (s *Server) PostData(ctx context.Context, message *Message) (*Response, error) {

	message.Path += ",Poster:[" + s.Hostname + "," + time.Now().Format("02-01-2006 - 15:04:05.0000") + "]"

	// Diretly convert (Marshal) the protobuf msg to json structure
	//	msgJSON, _ := protojson.Marshal(message)

	/***** OK, Lets get this into PostgreSQL *****/

	msgJSON, _ := protojson.Marshal(message)
	var dat map[string]interface{}

	if err := json.Unmarshal(msgJSON, &dat); err != nil {
		panic(err)
	}

	if message.Dest == "postgres" {

		// Execute the sql query - Rewrite this to accept a JSON strincture: insert into person JSON '?';
		//err := s.Postgres_dbConn.ExecutePostgresInsert("insert into person (uuid, seq, alpha, birthday, ccnumber, city, date, dollar, email, first, gender, last, latitude, longitude, note, path, state, street, zip) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)",

		vstmt := "insert into person (uuid, seq, alpha, birthday, ccnumber, city, date, dollar, email, first, gender, last, latitude, longitude, note, path, state, street, zip) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)"

		s.Postgres_dbConn.Insert(vstmt,
			message.Uuid,
			message.Seq,
			message.Alpha,
			message.Birthday,
			message.Ccnumber,
			message.City,
			message.Date,
			message.Dollar,
			message.Email,
			message.First,
			message.Gender,
			message.Last,
			message.Latitude,
			message.Longitude,
			message.Note,
			message.Path,
			message.State,
			message.Street,
			message.Zip)

	} else if message.Dest == "redis" {

		// Execute the Redis key set;
		err := s.Redis_dbConn.SetKey(message.Uuid, dat, time.Minute*1)

		if err != nil {
			grpcLog.Errorf("Had a problem setting Redis record ", err)

		}

	} else if message.Dest == "mongodb" {

		// Execute the MongoDB document insert;
		err := s.Mongo_dbConn.StoreDoc(dat)

		if err != nil {
			grpcLog.Errorf("Had a problem inserting MongoDB Document ", err)

		}
		grpcLog.Errorf("MongoDB Collection Stored ")

	} else if message.Dest == "mariadb" {

		vstmt := "insert into json.PERSON (uuid, seq, alpha, birthday, ccnumber, city, date, dollar, email, first, gender, last, latitude, longitude, note, path, state, street, zip, payload) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"

		jSONvalue, _ := json.Marshal(message)

		s.Maria_dbConn.Insert(vstmt,
			message.Uuid,
			message.Seq,
			message.Alpha,
			message.Birthday,
			message.Ccnumber,
			message.City,
			message.Date,
			message.Dollar,
			message.Email,
			message.First,
			message.Gender,
			message.Last,
			message.Latitude,
			message.Longitude,
			message.Note,
			message.Path,
			message.State,
			message.Street,
			message.Zip,
			jSONvalue)

	} else {
		grpcLog.Info("!!! INVALID Dest !!!")

	}

	if s.Debug_level == 1 {
		grpcLog.Info("Hostname: ", s.Hostname, " Dest: ", message.Dest, " Message: ", message.Seq, " : ", message.Uuid)

	} else if s.Debug_level == 2 {
		grpcLog.Info("Hostname: ", s.Hostname, " Dest: ", message.Dest)
		prettyPrintJSON(message)

	} else {
		grpcLog.Info("don't know what to print")

	}

	// Build response message
	response := &Response{
		Uuid: message.Uuid,
		Path: message.Path,
		Note: "Processed",
	}

	return response, nil

}

// Pretty Print JSON string
func prettyPrintJSON(v interface{}) {
	tmpBArray, err := json.MarshalIndent(v, "", "    ")
	if err == nil {
		grpcLog.Infof("Message:\n%s\n", tmpBArray)

	} else {
		grpcLog.Error("Really!?!? How is this possible:", err)
		return

	}
}

/*
* * * PostgreSQL Table * * *
*
*

	create table person (
		id SERIAL ,
	 	uuid text primary key,
	 	path text,
	 	seq text,
	 	alpha text,
	 	last text,
	 	first text,
	 	birthday text,
	 	gender text,
	 	email text,
	 	street text,
	 	state text,
	 	city text,
	 	zip text,
	 	ccnumber text,
	 	date text,
	 	latitude text,
	 	longitude text,
	 	dollar text,
	 	note text
	);



* * * MariaDB Table * * *
*
*	JSON Column Type
*	https://mariadb.com/kb/en/json-data-type/
*
********************************

	drop database if exists `json`;
	CREATE DATABASE IF NOT EXISTS `json` ;
	USE `json`;

	CREATE USER 'json'@localhost IDENTIFIED BY 'gbkb81iOal';
	GRANT ALL PRIVILEGES ON 'json'.* TO 'json'@localhost;


	-- Dumping structure for table json.person
	CREATE TABLE IF NOT EXISTS PERSON (
		`id` int(11) NOT NULL AUTO_INCREMENT,
		`uuid` varchar(100),
		`path` varchar(300),
		`seq` varchar(16),
		`alpha` varchar(30),
		`last` varchar(50),
		`first` varchar(50),
		`birthday` varchar(20),
		`gender` varchar(10),
		`email` varchar(50),
		`street` varchar(50),
		`state` varchar(30),
		`city` varchar(30),
		`zip` varchar(20),
		`ccnumber` varchar(16),
		`date` varchar(16),
		`latitude` varchar(20),
		`longitude` varchar(20),
		`dollar` varchar(20),
		`note` varchar(20),
		`payload` JSON DEFAULT '{"status": "empty"}',
		`createddate` timestamp(6) NOT NULL DEFAULT current_timestamp(6),
		`accountstatus` tinyint(1) DEFAULT 1,
		`balance` DOUBLE(18,5) NOT NULL DEFAULT '1000',
		PRIMARY KEY (`uuid`),
		KEY `person_idx` (`id`)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8;



*/
