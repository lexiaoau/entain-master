package main

import (
	"context"
	"flag"
	"net/http"

	"git.neds.sh/matty/entain/api/proto/event"
	"git.neds.sh/matty/entain/api/proto/racing"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var (
	apiEndpoint       = flag.String("api-endpoint", "localhost:8057", "API endpoint")
	grpcEndpoint      = flag.String("grpc-endpoint", "localhost:9000", "gRPC server endpoint")
	grpcEventEndpoint = flag.String("grpc-event-endpoint", "localhost:9001", "gRPC event server endpoint")
)

func main() {
	flag.Parse()

	if err := run(); err != nil {
		log.Fatalf("failed running api server: %s", err)
	}
}

func run() error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	mux := runtime.NewServeMux()
	if err := racing.RegisterRacingHandlerFromEndpoint(
		ctx,
		mux,
		*grpcEndpoint,
		[]grpc.DialOption{grpc.WithInsecure()},
	); err != nil {
		return err
	}

	if err := event.RegisterSportHandlerFromEndpoint(
		ctx,
		mux,
		*grpcEventEndpoint,
		[]grpc.DialOption{grpc.WithInsecure()},
	); err != nil {
		return err
	}

	log.Infof("API server listening on: %s", *apiEndpoint)

	return http.ListenAndServe(*apiEndpoint, mux)
}
