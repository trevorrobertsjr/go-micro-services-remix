package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/harlow/go-micro-services/registry"
	"github.com/harlow/go-micro-services/services/geo"
	"github.com/harlow/go-micro-services/tune"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/trevorrobertsjr/datadogwriter"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func main() {
	tune.Init()

	// Configure tracing
	tracer.Start(
		tracer.WithEnv("aiopslab"),
		tracer.WithService("geo"),
		tracer.WithServiceVersion("1.0"),
	)
	defer tracer.Stop() // Ensure tracer flushes before exit.

	// Configure logger
	datadogWriter := &datadogwriter.DatadogWriter{
		Service:  "geo",
		Hostname: "localhost",
		Tags:     "env:aiopslab,version:1.0",
		Source:   "geo-service",
	}
	log.Logger = zerolog.New(io.MultiWriter(os.Stdout, datadogWriter)).With().Timestamp().Logger()

	// Root context with cancellation for the service lifecycle
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Info().Msg("Reading config locally...")
	jsonFile, err := os.Open("config.json")
	if err != nil {
		log.Error().Msgf("Got error while reading config: %v", err)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)
	var result map[string]string
	json.Unmarshal([]byte(byteValue), &result)

	log.Info().Msgf("Read database URL: %v", result["GeoMongoAddress"])
	log.Info().Msg("Initializing DB connection...")

	// Trace MongoDB initialization
	// dbInitSpan, ctx := tracer.StartSpanFromContext(ctx, "geo.initializeDatabase", tracer.ResourceName("MongoDB Connection"))
	// dbInitSpan.SetTag(ext.DBInstance, "GeoMongo")
	mongoSession := initializeDatabase(ctx, result["GeoMongoAddress"])
	defer mongoSession.Close()
	// dbInitSpan.Finish()

	log.Info().Msg("Database connection successful.")

	servPort, _ := strconv.Atoi(result["GeoPort"])
	servIP := result["GeoIP"]

	log.Info().Msgf("Read target port: %v", servPort)
	log.Info().Msgf("Read consul address: %v", result["consulAddress"])

	consulAddr := flag.String("consuladdr", result["consulAddress"], "Consul address")
	flag.Parse()

	log.Info().Msgf("Initializing consul agent [host: %v]...", *consulAddr)
	registry, err := registry.NewClient(*consulAddr)
	if err != nil {
		log.Panic().Msgf("Got error while initializing consul agent: %v", err)
	}
	log.Info().Msg("Consul agent initialized.")

	srv := &geo.Server{
		Port:         servPort,
		IpAddr:       servIP,
		Registry:     registry,
		MongoSession: mongoSession,
	}

	// // Trace server run
	// serverRunSpan, ctx := tracer.StartSpanFromContext(ctx, "geo.server.Run", tracer.ResourceName("GeoServer"))
	// defer serverRunSpan.Finish()

	log.Info().Msg("Starting server...")
	if err := srv.Run(); err != nil {
		// serverRunSpan.SetTag(ext.Error, true)
		// serverRunSpan.SetTag("error.message", err.Error())
		log.Fatal().Msg(err.Error())
	}
}
