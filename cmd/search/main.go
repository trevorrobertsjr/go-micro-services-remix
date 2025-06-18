package main

import (
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/harlow/go-micro-services/registry"
	"github.com/harlow/go-micro-services/services/search"
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
		tracer.WithService("search"),
		tracer.WithServiceVersion("1.0"),
	)
	// Ensure tracing data is flushed to Datadog
	defer tracer.Stop()

	// Create root context for the application
	// ctx := context.Background()
	// rootSpan, ctx := tracer.StartSpanFromContext(rootCtx, "search.main", tracer.ResourceName("MainFunction"))
	// defer rootSpan.Finish()

	// Configure logging
	datadogWriter := &datadogwriter.DatadogWriter{
		Service:  "search",
		Hostname: "localhost",
		Tags:     "env:aiopslab,version:1.0",
		Source:   "search-service",
	}
	log.Logger = zerolog.New(io.MultiWriter(os.Stdout, datadogWriter)).With().Timestamp().Logger()

	log.Info().Msg("Reading config...")
	jsonFile, err := os.Open("config.json")
	if err != nil {
		log.Error().Msgf("Got error while reading config: %v", err)
		// rootSpan.SetTag(ext.Error, true)
		// rootSpan.SetTag("error.message", err.Error())
		return
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var result map[string]string
	if err := json.Unmarshal(byteValue, &result); err != nil {
		log.Error().Msgf("Failed to parse config: %v", err)
		// rootSpan.SetTag(ext.Error, true)
		// rootSpan.SetTag("error.message", err.Error())
		return
	}

	serv_port, _ := strconv.Atoi(result["SearchPort"])
	serv_ip := result["SearchIP"]
	knative_dns := result["KnativeDomainName"]

	log.Info().Msgf("Read target port: %v", serv_port)
	log.Info().Msgf("Read consul address: %v", result["consulAddress"])

	var consuladdr = flag.String("consuladdr", result["consulAddress"], "Consul address")
	flag.Parse()

	log.Info().Msgf("Initializing consul agent [host: %v]...", *consuladdr)
	registry, err := registry.NewClient(*consuladdr)
	if err != nil {
		log.Panic().Msgf("Got error while initializing consul agent: %v", err)
		// rootSpan.SetTag(ext.Error, true)
		// rootSpan.SetTag("error.message", err.Error())
		return
	}
	log.Info().Msg("Consul agent initialized")

	// Initialize and run the server
	srv := &search.Server{
		Port:       serv_port,
		IpAddr:     serv_ip,
		KnativeDns: knative_dns,
		Registry:   registry,
	}

	log.Info().Msg("Starting server...")
	if err := srv.Run(); err != nil {
		// rootSpan.SetTag(ext.Error, true)
		// rootSpan.SetTag("error.message", err.Error())
		log.Fatal().Msg(err.Error())
	}
}
