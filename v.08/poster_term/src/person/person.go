package person

import (
	"encoding/json"
	"os"
	"time"

	"golang.org/x/net/context"
	glog "google.golang.org/grpc/grpclog"
)

var grpcLog glog.LoggerV2

func init() {
	grpcLog = glog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)
}

type Server struct {
}

type messageJSON struct {
	Uuid      string `json:"uuid"`
	Path      string `json:"path"`
	Seq       string `json:"seq"`
	Alpha     string `json:"alpha"`
	Last      string `json:"last"`
	First     string `json:"first"`
	Birthday  string `json:"birthday"`
	Gender    string `json:"gender"`
	Email     string `json:"email"`
	Street    string `json:"street"`
	State     string `json:"state"`
	City      string `json:"city"`
	Zip       string `json:"zip"`
	Ccnumber  string `json:"ccnumber"`
	Date      string `json:"date"`
	Latitude  string `json:"latitude"`
	Longitude string `json:"longitude"`
	Dollar    string `json:"dollar"`
	Note      string `json:"note"`
}

func (s *Server) PostData(ctx context.Context, message *Message) (*Response, error) {

	// Lets identify ourself
	var vHostname, e1 = os.Hostname()
	if e1 != nil {
		grpcLog.Error("Can't retrieve hostname", e1)
	}

	msgJSON := &messageJSON{
		Uuid:      message.Uuid,
		Path:      message.Path,
		Seq:       message.Seq,
		Alpha:     message.Alpha,
		Last:      message.Last,
		First:     message.First,
		Birthday:  message.Birthday,
		Gender:    message.Gender,
		Email:     message.Email,
		Street:    message.Street,
		State:     message.State,
		City:      message.City,
		Zip:       message.Zip,
		Ccnumber:  message.Ccnumber,
		Date:      message.Date,
		Latitude:  message.Latitude,
		Longitude: message.Longitude,
		Dollar:    message.Dollar,
		Note:      message.Note,
	}

	msgJSON.Path += ",Poster_Term:[" + vHostname + "," + time.Now().Format("02-01-2006 - 15:04:05.0000") + "]"

	prettyPrintJSON(msgJSON)

	// Now this is where we're going to be inserting/updating aka upserting into the various DB's.
	/*
					https://www.bmc.com/blogs/using-json-with-cassandra/


					create keyspace json with REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 2 };

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

	*/

	response := &Response{
		Uuid: msgJSON.Uuid,
		Path: msgJSON.Path,
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
