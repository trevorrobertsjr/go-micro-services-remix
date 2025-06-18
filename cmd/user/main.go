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
	"github.com/harlow/go-micro-services/services/user"
	"github.com/harlow/go-micro-services/tune"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/trevorrobertsjr/datadogwriter"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func main() {
	tune.Init()

	// Initialize root context
	ctx := context.Background()

	// Configure tracing
	tracer.Start(
		tracer.WithEnv("aiopslab"),
		tracer.WithService("user"),
		tracer.WithServiceVersion("1.0"),
	)
	// Ensure tracer flushes on exit
	defer tracer.Stop()

	// Configure logging
	datadogWriter := &datadogwriter.DatadogWriter{
		Service:  "user",
		Hostname: "localhost",
		Tags:     "env:aiopslab,version:1.0",
		Source:   "user-service",
	}
	log.Logger = zerolog.New(io.MultiWriter(os.Stdout, datadogWriter)).With().Timestamp().Logger()

	// // Start main span
	// mainSpan, ctx := tracer.StartSpanFromContext(ctx, "user.main", tracer.ResourceName("Main"))
	// defer mainSpan.Finish()

	log.Info().Msg("Reading config...")
	jsonFile, err := os.Open("config.json")
	if err != nil {
		// mainSpan.SetTag(ext.Error, true)
		log.Panic().Msgf("Got error while reading config: %v", err)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var result map[string]string
	if err := json.Unmarshal([]byte(byteValue), &result); err != nil {
		// mainSpan.SetTag(ext.Error, true)
		log.Panic().Msgf("Error parsing config: %v", err)
	}

	log.Info().Msgf("Read database URL: %v", result["UserMongoAddress"])
	log.Info().Msg("Initializing DB connection...")
	// dbSpan, ctx := tracer.StartSpanFromContext(ctx, "user.initializeDatabase", tracer.ResourceName("InitializeDatabase"))
	mongoSession := initializeDatabase(ctx, result["UserMongoAddress"])
	// dbSpan.Finish()
	defer mongoSession.Close()

	log.Info().Msg("Database connection successful")

	servPort, _ := strconv.Atoi(result["UserPort"])
	servIP := result["UserIP"]

	log.Info().Msgf("Read target port: %v", servPort)
	log.Info().Msgf("Read consul address: %v", result["consulAddress"])

	var (
		consulAddr = flag.String("consuladdr", result["consulAddress"], "Consul address")
	)
	flag.Parse()

	log.Info().Msgf("Initializing consul agent [host: %v]...", *consulAddr)
	// regSpan, ctx := tracer.StartSpanFromContext(ctx, "user.registry", tracer.ResourceName("RegistryClient"))
	registryClient, err := registry.NewClient(*consulAddr)
	// regSpan.Finish()
	if err != nil {
		// mainSpan.SetTag(ext.Error, true)
		log.Panic().Msgf("Got error while initializing consul agent: %v", err)
	}
	log.Info().Msg("Consul agent initialized")

	srv := &user.Server{
		Registry:     registryClient,
		Port:         servPort,
		IpAddr:       servIP,
		MongoSession: mongoSession,
	}

	log.Info().Msg("Starting server...")
	// serverSpan, ctx := tracer.StartSpanFromContext(ctx, "user.server.Run", tracer.ResourceName("Run"))
	if err := srv.Run(); err != nil {
		// serverSpan.SetTag(ext.Error, true)
		// serverSpan.SetTag("error.message", err.Error())
		log.Fatal().Msg(err.Error())
	}
	// serverSpan.Finish()
}
