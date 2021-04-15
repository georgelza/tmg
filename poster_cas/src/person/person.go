package person

import (
	"encoding/json"
	"os"
	"time"

	"github.com/georgelza/tmg_poster_cas/database"

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

	/***** OK, LKets get this into Cassandra *****/

	// Execute the cql query - Rewrite this to accept a JSON strincture: insert into person JSON '?';
	err := s.DBConn.ExecuteQuery("insert into person (uuid, seq, alpha, birthday, ccnumber, city, date, dollar, email, first, gender, last, latitude, longitude, note, path, state, street, zip) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);",
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

			https://www.bmc.com/blogs/using-json-with-cassandra/


				create keyspace json with REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

				create table books (isbn text primary key, title text, publisher text);

				insert into books JSON '{"isbn": "123",
					"title": "The Magic Mountain", "publisher": "Knofp"}';

				select * from books;


				use json;
				CREATE type json.sale ( id int, item text, amount int );
				CREATE TABLE json.customers ( id int  PRIMARY KEY, name text, balance int, sales list> );

				INSERT INTO json.customers (id, name, balance, sales)
				VALUES (123, 'Greenville Hardware', 700,
			 	[{ id: 5544, item : 'tape', amount : 100},
			  	{ id: 5545, item : 'wire', amount : 200}]) ;

				  select * from customers;

				  select id, toJson(sales) from customers;

				  https://docs.datastax.com/en/cql-oss/3.3/cql/cql_using/useInsertJSON.html

				  INSERT INTO cycling.cyclist_category JSON '{
	  				"category" : "GC",
	  				"points" : 780,
					"id" : "829aa84a-4bba-411f-a4fb-38167a987cda",
	  				"lastname" : "SUTHERLAND" }';


				create keyspace json with REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };

				create table person (
				 	uuid text,
				 	path text,
				 	seq text primary key,
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
				 	note text,
				);

				insert into person json '{
	             	"uuid": "2fcbff07-4530-426d-9be0-5c1952dfc4c7",
	             	"path": "FileGester:[Georges-MacBook-Pro.local,08-04-2021 - 20:43:45.5940],Scrubber:[Georges-MacBook-Pro.local,08-04-2021 - 20:43:45.6034],Poster_Term:[Georges-MacBook-Pro.local,08-04-2021 - 20:43:45.6045]",
	             	"seq": "1",
	             	"alpha": "KrtRq",
	             	"last": "Elsie",
	             	"first": "Hernandez",
	             	"birthday": "10/27/1991",
	             	"gender": "Male",
	             	"email": "zo@geb.tj",
	             	"street": "Kaldo View",
	             	"state": "FL",
	             	"city": "Huturor",
	             	"zip": "20870",
	             	"ccnumber": "201476934770701",
	             	"date": "11/24/1968",
	             	"latitude": "31.18237",
	             	"longitude": "78.29411",
	             	"dollar": "$2857.98",
	             	"note": ""
	         	}';

				insert into person JSON '?';

*/
