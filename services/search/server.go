package search

import (
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/harlow/go-micro-services/dialer"
	"github.com/harlow/go-micro-services/registry"
	geo "github.com/harlow/go-micro-services/services/geo/proto"
	rate "github.com/harlow/go-micro-services/services/rate/proto"
	pb "github.com/harlow/go-micro-services/services/search/proto"
	"github.com/harlow/go-micro-services/tls"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/stats"
	ddgrpc "gopkg.in/DataDog/dd-trace-go.v1/contrib/google.golang.org/grpc"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const name = "srv-search"

// tracerStatsHandler implements gRPC stats.Handler for Datadog tracing.
type tracerStatsHandler struct{}

func (t *tracerStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	span, ctx := tracer.StartSpanFromContext(ctx, info.FullMethodName, tracer.ResourceName("grpc"))
	return tracer.ContextWithSpan(ctx, span)
}

func (t *tracerStatsHandler) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
	span, ok := tracer.SpanFromContext(ctx)
	if !ok {
		return
	}

	// Handle specific RPC stats events
	switch statsEvent := rpcStats.(type) {
	case *stats.InPayload:
		span.SetTag("event", "in_payload")
		span.SetTag("bytes_received", statsEvent.Length)
	case *stats.OutPayload:
		span.SetTag("event", "out_payload")
		span.SetTag("bytes_sent", statsEvent.Length)
	case *stats.End:
		if statsEvent.Error != nil {
			span.SetTag(ext.Error, statsEvent.Error)
		}
	default:
		span.SetTag("event", "unknown")
	}

	span.Finish()
}

func (t *tracerStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (t *tracerStatsHandler) HandleConn(context.Context, stats.ConnStats) {}

func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		span, ctx := tracer.StartSpanFromContext(ctx, info.FullMethod, tracer.ResourceName(info.FullMethod))
		defer span.Finish()

		resp, err := handler(ctx, req)
		if err != nil {
			span.SetTag(ext.Error, true)
			span.SetTag("error.message", err.Error())
		}

		return resp, err
	}
}

func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		span, ctx := tracer.StartSpanFromContext(ss.Context(), info.FullMethod, tracer.ResourceName(info.FullMethod))
		defer span.Finish()

		wrappedStream := &serverStreamWrapper{
			ServerStream: ss,
			ctx:          ctx,
		}

		err := handler(srv, wrappedStream)
		if err != nil {
			span.SetTag(ext.Error, true)
			span.SetTag("error.message", err.Error())
		}

		return err
	}
}

type serverStreamWrapper struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *serverStreamWrapper) Context() context.Context {
	return w.ctx
}

// Server implements the search service
type Server struct {
	geoClient  geo.GeoClient
	rateClient rate.RateClient
	Port       int
	IpAddr     string
	KnativeDns string
	Registry   *registry.Client
	uuid       string
}

// Run starts the server
func (s *Server) Run() error {
	// span, ctx := tracer.StartSpanFromContext(ctx, "search.Run", tracer.ResourceName("Run"))
	// defer span.Finish()

	if s.Port == 0 {
		// span.SetTag(ext.Error, true)
		return fmt.Errorf("server port must be set")
	}

	s.uuid = uuid.New().String()

	// opts := []grpc.ServerOption{
	// 	grpc.KeepaliveParams(keepalive.ServerParameters{
	// 		Timeout: 120 * time.Second,
	// 	}),
	// 	grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
	// 		PermitWithoutStream: true,
	// 	}),
	// 	grpc.StatsHandler(&tracerStatsHandler{}),          // Datadog tracing
	// 	grpc.UnaryInterceptor(UnaryServerInterceptor()),   // Add unary interceptor
	// 	grpc.StreamInterceptor(StreamServerInterceptor()), // Add stream interceptor
	// }

	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Timeout: 120 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			PermitWithoutStream: true,
		}),
		grpc.UnaryInterceptor(ddgrpc.UnaryServerInterceptor()),   // Native Datadog unary interceptor
		grpc.StreamInterceptor(ddgrpc.StreamServerInterceptor()), // Native Datadog stream interceptor
	}

	if tlsopt := tls.GetServerOpt(); tlsopt != nil {
		opts = append(opts, tlsopt)
	}

	srv := grpc.NewServer(opts...)
	pb.RegisterSearchServer(srv, s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		// span.SetTag(ext.Error, true)
		log.Fatal().Msgf("Failed to listen: %v", err)
	}

	// Construct service DNS address without the prefix
	namespace := "test-hotel-reservation"                                     // Replace with your namespace
	serviceDNS := fmt.Sprintf("%s.%s.svc.cluster.local", name[4:], namespace) // Strip "srv-" from `name`

	log.Info().Msgf("Registering service [name: %s, id: %s, address: %s, port: %d]", name, s.uuid, serviceDNS, s.Port)
	err = s.Registry.Register(name, s.uuid, serviceDNS, s.Port)
	if err != nil {
		// span.SetTag(ext.Error, true)
		return fmt.Errorf("failed to register: %v", err)
	}
	log.Info().Msg("Successfully registered in consul")

	return srv.Serve(lis)
}

// Shutdown cleans up any processes
func (s *Server) Shutdown() {
	s.Registry.Deregister(s.uuid)
}

func (s *Server) initGeoClient(ctx context.Context, name string) error {
	if s.geoClient != nil {
		return nil // Already initialized
	}

	conn, err := s.getGrpcConn(ctx, name)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.geoClient = geo.NewGeoClient(conn)
	return nil
}

func (s *Server) initRateClient(ctx context.Context, name string) error {
	if s.rateClient != nil {
		return nil // Already initialized
	}

	conn, err := s.getGrpcConn(ctx, name)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.rateClient = rate.NewRateClient(conn)
	return nil
}

// func (s *Server) getGrpcConn(ctx context.Context, name string) (*grpc.ClientConn, error) {
// 	span, ctx := tracer.StartSpanFromContext(ctx, "getGrpcConn", tracer.ResourceName("Dial gRPC Connection"))
// 	defer span.Finish()

// 	var target string
// 	if s.KnativeDns != "" {
// 		target = fmt.Sprintf("%s.%s", name, s.KnativeDns)
// 		log.Info().Msgf("Dialing Knative DNS target: %s", target)
// 	} else {
// 		target = fmt.Sprintf("consul:///%s", name)
// 		log.Info().Msgf("Dialing Consul target: %s", target)
// 	}

// 	// Use the updated Dial function with the unary interceptor
// 	conn, err := dialer.Dial(ctx, target,
// 		grpc.WithStatsHandler(&dialer.TracerStatsHandler{}), // Existing stats handler
// 		dialer.WithUnaryInterceptor(),                       // Add unary interceptor
// 	)
// 	if err != nil {
// 		span.SetTag(ext.Error, true)
// 		span.SetTag("error.message", err.Error())
// 		log.Error().Msgf("Failed to dial target %s: %v", target, err)
// 		return nil, err
// 	}

// 	span.SetTag("grpc.target", target)
// 	log.Info().Msgf("Successfully connected to gRPC target: %s", target)
// 	return conn, nil
// }

func (s *Server) getGrpcConn(ctx context.Context, name string) (*grpc.ClientConn, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "getGrpcConn", tracer.ResourceName("Dial gRPC Connection"))
	defer span.Finish()

	var target string
	if s.KnativeDns != "" {
		target = fmt.Sprintf("%s.%s", name, s.KnativeDns)
		log.Info().Msgf("Dialing Knative DNS target: %s", target)
	} else {
		target = fmt.Sprintf("consul:///%s", name)
		log.Info().Msgf("Dialing Consul target: %s", target)
	}

	conn, err := dialer.Dial(ctx, target, dialer.WithUnaryInterceptor())
	if err != nil {
		span.SetTag(ext.Error, true)
		span.SetTag("error.message", err.Error())
		log.Error().Msgf("Failed to dial target %s: %v", target, err)
		return nil, err
	}

	span.SetTag("grpc.target", target)
	log.Info().Msgf("Successfully connected to gRPC target: %s", target)
	return conn, nil
}

// Nearby returns IDs of nearby hotels ordered by ranking algo
func (s *Server) Nearby(ctx context.Context, req *pb.NearbyRequest) (*pb.SearchResult, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "search.Nearby", tracer.ResourceName("Nearby"))
	defer span.Finish()

	if err := s.initGeoClient(ctx, "srv-geo"); err != nil {
		span.SetTag(ext.Error, true)
		return nil, err
	}

	if err := s.initRateClient(ctx, "srv-rate"); err != nil {
		span.SetTag(ext.Error, true)
		return nil, err
	}

	nearby, err := s.geoClient.Nearby(ctx, &geo.Request{
		Lat: req.Lat,
		Lon: req.Lon,
	})
	if err != nil {
		span.SetTag(ext.Error, true)
		span.SetTag("geo.error", err.Error())
		return nil, err
	}

	rates, err := s.rateClient.GetRates(ctx, &rate.Request{
		HotelIds: nearby.HotelIds,
		InDate:   req.InDate,
		OutDate:  req.OutDate,
	})
	if err != nil {
		span.SetTag(ext.Error, true)
		return nil, err
	}

	res := new(pb.SearchResult)
	for _, ratePlan := range rates.RatePlans {
		res.HotelIds = append(res.HotelIds, ratePlan.HotelId)
	}
	return res, nil
}
