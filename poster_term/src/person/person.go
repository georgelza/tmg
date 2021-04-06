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
	Id        string `json:"id"`
	Timestamp string `json:"timestamp"`
	Source    string `json:"source"`
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
	Body      string `json:"body"`
}

func (s *Server) PostData(ctx context.Context, message *Message) (*Message, error) {

	// Lets identify ourself
	var vHostname, e1 = os.Hostname()
	if e1 != nil {
		grpcLog.Error("Can't retrieve hostname", e1)
	}

	msgJSON := &messageJSON{
		Id:        message.Id,
		Timestamp: message.Timestamp,
		Source:    message.Source,
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
		Body:      message.Body,
	}

	msgJSON.Timestamp += "(" + time.Now().Format("02-01-2006 - 15:04:05.0000") + ")"
	msgJSON.Source += "(" + vHostname + ")"

	prettyPrintJSON(msgJSON)

	// Now this is where we're going to be inserting/updating aka upserting into the various DB's.
	//
	//
	//

	return &Message{Body: "Heelo from the Server! :"}, nil

}

// Pretty Print JSON string
func prettyPrintJSON(v interface{}) {
	tmpBArray, err := json.MarshalIndent(v, "", "    ")
	if err == nil {
		//log.Printf("Message:\n%s\n", tmpBArray)
		grpcLog.Infof("Message:\n%s\n", tmpBArray)

		//fmt.Print("Message: ", tmpBArray, "\n\n") // raw serialized
		//grpcLog.Info("Message:\n%s\n", tmpBArray) // raw serialized

	} else {
		grpcLog.Error("Really!?!? How is this possible:", err)
		return

	}
}
