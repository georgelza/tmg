package person

import (
	"log"

	"golang.org/x/net/context"
)

type Server struct {
}

func (s *Server) PostData(ctx context.Context, message *Message) (*Message, error) {
	log.Printf("Received message body from client: %s", message.Seq)

	return &Message{Body: "Heelo from the Server!"}, nil

}
