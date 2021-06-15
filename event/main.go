package main

import (
	"database/sql"
	"flag"
	"git.neds.sh/matty/entain/event/db"
	"git.neds.sh/matty/entain/event/proto/event"
	"git.neds.sh/matty/entain/event/service"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
)

var (
	grpcEventEndpoint = flag.String("grpc-event-endpoint", "localhost:9001", "gRPC event server endpoint")
)

func main() {
	flag.Parse()

	if err := run(); err != nil {
		log.Fatalf("failed running grpc server: %s", err)
	}
}

func run() error {
	conn, err := net.Listen("tcp", ":9001")
	if err != nil {
		return err
	}

	eventDB, err := sql.Open("sqlite3", "./db/event.db")
	if err != nil {
		return err
	}

	eventRepo := db.NewEventsRepo(eventDB)
	if err := eventRepo.Init(); err != nil {
		return err
	}

	grpcServer := grpc.NewServer()

	event.RegisterSportServer(
		grpcServer,
		service.NewEventService(
			eventRepo,
		),
	)

	log.Infof("gRPC server listening on: %s", *grpcEventEndpoint)

	if err := grpcServer.Serve(conn); err != nil {
		return err
	}

	return nil
}
