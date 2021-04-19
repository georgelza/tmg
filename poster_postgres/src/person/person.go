package person

import (
	"encoding/json"
	"os"
	"time"

	"github.com/georgelza/tmg_poster_postgres/database"

	"golang.org/x/net/context"
	glog "google.golang.org/grpc/grpclog"
)

var grpcLog glog.LoggerV2

func init() {
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)
}

type Server struct {
	DBConn      *database.DBConnection
	Debug_level int
	Hostname    string
}

func (s *Server) PostData(ctx context.Context, message *Message) (*Response, error) {

	message.Path += ",Poster_Term:[" + s.Hostname + "," + time.Now().Format("02-01-2006 - 15:04:05.0000") + "]"

	// Diretly convert (Marshal) the protobuf msg to json structure
	//	msgJSON, _ := protojson.Marshal(message)

	/***** OK, Lets get this into PostgreSQL *****/

	// Execute the sql query - Rewrite this to accept a JSON strincture: insert into person JSON '?';
	err := s.DBConn.ExecuteInsert("insert into person (uuid, seq, alpha, birthday, ccnumber, city, date, dollar, email, first, gender, last, latitude, longitude, note, path, state, street, zip) values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)",
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

	if err != nil {
		grpcLog.Fatal("Had a problem inserting record ", err)

	}

	if s.Debug_level == 2 {
		grpcLog.Info("Hostname : ", s.Hostname, " Message: ", message.Seq, " : ", message.Uuid)

	} else if s.Debug_level == 3 {
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

	CREATE TABLE users (
  		id SERIAL PRIMARY KEY,
  		age INT,
  		first_name TEXT,
  		last_name TEXT,
  		email TEXT UNIQUE NOT NULL
);

*/
