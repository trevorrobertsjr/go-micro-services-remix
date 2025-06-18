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
	"github.com/harlow/go-micro-services/services/profile"
	"github.com/harlow/go-micro-services/tune"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/trevorrobertsjr/datadogwriter"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func main() {
	tune.Init()
	// Create root context
	ctx := context.Background()

	// Configure tracing
	tracer.Start(
		tracer.WithEnv("aiopslab"),
		tracer.WithService("profile"),
		tracer.WithServiceVersion("1.0"),
	)
	defer tracer.Stop()

	// Configure logger
	datadogWriter := &datadogwriter.DatadogWriter{
		Service:  "profile",
		Hostname: "localhost",
		Tags:     "env:aiopslab,version:1.0",
		Source:   "profile-service",
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

	log.Info().Msgf("Read database URL: %v", result["ProfileMongoAddress"])
	log.Info().Msg("Initializing DB connection...")
	mongo_session := initializeDatabase(ctx, result["ProfileMongoAddress"])
	defer mongo_session.Close()
	log.Info().Msg("Successfull")

	log.Info().Msgf("Read profile memcashed address: %v", result["ProfileMemcAddress"])
	log.Info().Msg("Initializing Memcached client...")
	memc_client := tune.NewMemCClient2(result["ProfileMemcAddress"])
	log.Info().Msg("Successfull")

	serv_port, _ := strconv.Atoi(result["ProfilePort"])
	serv_ip := result["ProfileIP"]
	log.Info().Msgf("Read target port: %v", serv_port)
	log.Info().Msgf("Read consul address: %v", result["consulAddress"])

	var consuladdr = flag.String("consuladdr", result["consulAddress"], "Consul address")
	flag.Parse()

	log.Info().Msgf("Initializing consul agent [host: %v]...", *consuladdr)
	registry, err := registry.NewClient(*consuladdr)
	if err != nil {
		log.Panic().Msgf("Got error while initializing consul agent: %v", err)
	}
	log.Info().Msg("Consul agent initialized")

	srv := profile.Server{
		Registry:     registry,
		Port:         serv_port,
		IpAddr:       serv_ip,
		MongoSession: mongo_session,
		MemcClient:   memc_client,
	}

	// // Trace server run
	// serverRunSpan, ctx := tracer.StartSpanFromContext(ctx, "profile.server.Run", tracer.ResourceName("ProfileServer"))
	// defer serverRunSpan.Finish()

	log.Info().Msg("Starting server...")
	if err := srv.Run(); err != nil {
		// serverRunSpan.SetTag(ext.Error, true)
		// serverRunSpan.SetTag("error.message", err.Error())
		log.Fatal().Msg(err.Error())
	}
}
