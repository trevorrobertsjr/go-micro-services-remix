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
	"github.com/harlow/go-micro-services/services/frontend"
	"github.com/harlow/go-micro-services/tune"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/trevorrobertsjr/datadogwriter"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func main() {
	// Initialize configuration and tuning parameters
	tune.Init()

	// Configure tracing
	tracer.Start(
		tracer.WithEnv("aiopslab"),       // Environment tag
		tracer.WithService("frontend"),   // Service name
		tracer.WithServiceVersion("1.0"), // Service version
		// tracer.WithGlobalTag("team", "platform"), // Custom global tags
	)
	defer tracer.Stop() // Ensure traces are flushed before the application exits

	// Configure logger with Datadog writer
	datadogWriter := &datadogwriter.DatadogWriter{
		Service:  "frontend",
		Hostname: "localhost",
		Tags:     "env:aiopslab,version:1.0",
		Source:   "frontend-service",
	}
	log.Logger = zerolog.New(io.MultiWriter(os.Stdout, datadogWriter)).With().Timestamp().Logger()

	// Read configuration
	log.Info().Msg("Reading configuration...")
	jsonFile, err := os.Open("config.json")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to read config file")
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)
	var result map[string]string
	if err := json.Unmarshal(byteValue, &result); err != nil {
		log.Fatal().Err(err).Msg("Failed to parse config file")
	}

	// Extract configurations
	servPort, _ := strconv.Atoi(result["FrontendPort"])
	servIP := result["FrontendIP"]
	knativeDNS := result["KnativeDomainName"]
	consulAddr := result["consulAddress"]

	log.Info().Msgf("Configuration: Port=%d, ConsulAddress=%s", servPort, consulAddr)

	// Parse flags
	flag.Parse()

	// Initialize Consul registry
	log.Info().Msgf("Initializing Consul agent at %s...", consulAddr)
	registry, err := registry.NewClient(consulAddr)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to initialize Consul agent")
	}
	log.Info().Msg("Consul agent initialized successfully")

	// Initialize the frontend server
	srv := &frontend.Server{
		KnativeDns: knativeDNS,
		Registry:   registry,
		IpAddr:     servIP,
		Port:       servPort,
	}

	// Pass root context to enable trace propagation
	rootCtx := context.Background()

	log.Info().Msg("Starting frontend server...")
	if err := srv.Run(rootCtx); err != nil {
		log.Fatal().Err(err).Msg("Frontend server encountered an error")
	}
}
