package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"time"

	"github.com/harlow/go-micro-services/registry"
	"github.com/harlow/go-micro-services/services/reservation"
	"github.com/harlow/go-micro-services/tune"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/trevorrobertsjr/datadogwriter"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func main() {
	tune.Init()

	// Root context with timeout for server operations
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Configure tracing
	tracer.Start(
		tracer.WithEnv("aiopslab"),
		tracer.WithService("reservation"),
		tracer.WithServiceVersion("1.0"),
	)
	defer tracer.Stop() // Ensure tracer flushes data before exiting.

	// Configure logging
	datadogWriter := &datadogwriter.DatadogWriter{
		Service:  "reservation",
		Hostname: "localhost",
		Tags:     "env:aiopslab,version:1.0",
		Source:   "reservation-service",
	}
	log.Logger = zerolog.New(io.MultiWriter(os.Stdout, datadogWriter)).With().Timestamp().Logger()

	log.Info().Msg("Reading config...")
	jsonFile, err := os.Open("config.json")
	if err != nil {
		log.Error().Msgf("Got error while reading config: %v", err)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var result map[string]string
	json.Unmarshal([]byte(byteValue), &result)

	log.Info().Msgf("Read database URL: %v", result["ReserveMongoAddress"])
	log.Info().Msg("Initializing DB connection...")
	mongoSession := initializeDatabase(ctx, result["ReserveMongoAddress"])
	defer mongoSession.Close()
	log.Info().Msg("Successful")

	log.Info().Msgf("Read profile memcached address: %v", result["ReserveMemcAddress"])
	log.Info().Msg("Initializing Memcached client...")
	memcClient := tune.NewMemCClient2(result["ReserveMemcAddress"])
	log.Info().Msg("Successful")

	servPort, _ := strconv.Atoi(result["ReservePort"])
	servIP := result["ReserveIP"]

	log.Info().Msgf("Read target port: %v", servPort)
	log.Info().Msgf("Read consul address: %v", result["consulAddress"])

	var (
		consulAddr = flag.String("consuladdr", result["consulAddress"], "Consul address")
	)
	flag.Parse()

	log.Info().Msgf("Initializing consul agent [host: %v]...", *consulAddr)
	registry, err := registry.NewClient(*consulAddr)
	if err != nil {
		log.Panic().Msgf("Got error while initializing consul agent: %v", err)
	}
	log.Info().Msg("Consul agent initialized")

	// // Start span for server initialization and running
	// serverSpan, ctx := tracer.StartSpanFromContext(ctx, "reservation.server.init", tracer.ResourceName("ReservationServer"))
	// defer serverSpan.Finish()

	srv := &reservation.Server{
		Registry:     registry,
		Port:         servPort,
		IpAddr:       servIP,
		MongoSession: mongoSession,
		MemcClient:   memcClient,
	}

	log.Info().Msg("Starting server...")
	if err := srv.Run(); err != nil {
		// serverSpan.SetTag(ext.Error, true)
		// serverSpan.SetTag("error.message", err.Error())
		log.Fatal().Msg(err.Error())
	}
}
