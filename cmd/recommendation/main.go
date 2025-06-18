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
	"github.com/harlow/go-micro-services/services/recommendation"
	"github.com/harlow/go-micro-services/tune"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/trevorrobertsjr/datadogwriter"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func main() {
	tune.Init()
	// Create root context for tracing
	ctx := context.Background()

	// Configure tracing
	tracer.Start(
		tracer.WithEnv("aiopslab"),
		tracer.WithService("recommendation"),
		tracer.WithServiceVersion("1.0"),
	)
	defer tracer.Stop()

	// Configure logging
	datadogWriter := &datadogwriter.DatadogWriter{
		Service:  "recommendation",
		Hostname: "localhost",
		Tags:     "env:aiopslab,version:1.0",
		Source:   "recommendation-service",
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

	log.Info().Msgf("Read database URL: %v", result["RecommendMongoAddress"])
	log.Info().Msg("Initializing DB connection...")
	mongoSession := initializeDatabase(ctx, result["RecommendMongoAddress"])
	defer mongoSession.Close()
	log.Info().Msg("Database initialization successful")

	servPort, _ := strconv.Atoi(result["RecommendPort"])
	servIP := result["RecommendIP"]

	log.Info().Msgf("Read target port: %v", servPort)
	log.Info().Msgf("Read consul address: %v", result["consulAddress"])

	var (
		consulAddr = flag.String("consuladdr", result["consulAddress"], "Consul address")
	)
	flag.Parse()

	log.Info().Msgf("Initializing consul agent [host: %v]...", *consulAddr)
	registry, err := registry.NewClient(*consulAddr)
	if err != nil {
		log.Panic().Msgf("Error initializing consul agent: %v", err)
	}
	log.Info().Msg("Consul agent initialized")

	srv := &recommendation.Server{
		Registry:     registry,
		Port:         servPort,
		IpAddr:       servIP,
		MongoSession: mongoSession,
	}

	// serverRunSpan, ctx := tracer.StartSpanFromContext(ctx, "recommendation.server.Run", tracer.ResourceName("RecommendationServer"))
	// defer serverRunSpan.Finish()

	log.Info().Msg("Starting server...")
	if err := srv.Run(); err != nil {
		// serverRunSpan.SetTag(ext.Error, true)
		// serverRunSpan.SetTag("error.message", err.Error())
		log.Fatal().Msg(err.Error())
	}
}
