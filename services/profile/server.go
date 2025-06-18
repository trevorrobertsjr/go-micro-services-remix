package profile

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/google/uuid"
	"github.com/harlow/go-micro-services/registry"
	pb "github.com/harlow/go-micro-services/services/profile/proto"
	"github.com/harlow/go-micro-services/tls"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/stats"
	ddgrpc "gopkg.in/DataDog/dd-trace-go.v1/contrib/google.golang.org/grpc"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const name = "srv-profile"

// tracerStatsHandler implements gRPC stats.Handler for Datadog tracing.
type tracerStatsHandler struct{}

func (t *tracerStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	span, ctx := tracer.StartSpanFromContext(ctx, info.FullMethodName, tracer.Tag(ext.SpanType, "rpc"))
	return tracer.ContextWithSpan(ctx, span)
}

func (t *tracerStatsHandler) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
	span, ok := tracer.SpanFromContext(ctx)
	if !ok {
		return
	}

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

// Server implements the profile service
type Server struct {
	uuid         string
	Port         int
	IpAddr       string
	MongoSession *mgo.Session
	Registry     *registry.Client
	MemcClient   *memcache.Client
}

// Run starts the server
func (s *Server) Run() error {
	if s.Port == 0 {
		return fmt.Errorf("server port must be set")
	}

	s.uuid = uuid.New().String()

	log.Trace().Msgf("Starting profile service at %s:%d", s.IpAddr, s.Port)

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
		grpc.UnaryInterceptor(ddgrpc.UnaryServerInterceptor()),   // Datadog unary interceptor
		grpc.StreamInterceptor(ddgrpc.StreamServerInterceptor()), // Datadog stream interceptor
	}

	if tlsopt := tls.GetServerOpt(); tlsopt != nil {
		opts = append(opts, tlsopt)
	}

	srv := grpc.NewServer(opts...)
	pb.RegisterProfileServer(srv, s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		log.Fatal().Msgf("Failed to configure listener: %v", err)
	}

	// Construct service DNS address without the prefix
	namespace := "test-hotel-reservation"                                     // Replace with your namespace
	serviceDNS := fmt.Sprintf("%s.%s.svc.cluster.local", name[4:], namespace) // Strip "srv-" from `name`

	log.Info().Msgf("Registering service [name: %s, id: %s, address: %s, port: %d]", name, s.uuid, serviceDNS, s.Port)

	// ** Add tracing span for service registration **
	// regSpan, ctx := tracer.StartSpanFromContext(ctx, "service.registration")
	err = s.Registry.Register(name, s.uuid, serviceDNS, s.Port)
	// regSpan.Finish()
	if err != nil {
		return fmt.Errorf("failed to register: %v", err)
	}

	log.Info().Msg("Successfully registered in consul")
	return srv.Serve(lis)
}

// Shutdown cleans up any processes
func (s *Server) Shutdown() {
	s.Registry.Deregister(s.uuid)
}

// GetProfiles returns hotel profiles for requested IDs
func (s *Server) GetProfiles(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "profile.GetProfiles")
	defer span.Finish()

	log.Trace().Msg("In GetProfiles")

	res := new(pb.Result)
	hotels := make([]*pb.Hotel, 0)
	var wg sync.WaitGroup
	var mutex sync.Mutex

	hotelIds := make([]string, 0)
	profileMap := make(map[string]struct{})
	for _, hotelId := range req.HotelIds {
		hotelIds = append(hotelIds, hotelId)
		profileMap[hotelId] = struct{}{}
	}

	// *** Updated: Add timeout and tracing for Memcached operation ***
	memCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	memSpan, memCtx := tracer.StartSpanFromContext(memCtx, "memcached.get_profile", tracer.Tag(ext.SpanType, "cache"))
	memSpan.SetTag(ext.Component, "memcached")
	memSpan.SetTag(ext.PeerService, "memcached-profile")
	resMap, err := s.MemcClient.GetMulti(hotelIds) // Memcached does not support context directly
	memSpan.Finish()
	if err != nil && err != memcache.ErrCacheMiss {
		log.Panic().Msgf("Memcached error: %v", err)
	}

	for hotelId, item := range resMap {
		hotelProf := new(pb.Hotel)
		json.Unmarshal(item.Value, hotelProf)
		hotels = append(hotels, hotelProf)
		delete(profileMap, hotelId)
	}

	wg.Add(len(profileMap))
	for hotelId := range profileMap {
		go func(hotelId string) {
			defer wg.Done()

			// *** Updated: Add timeout and tracing for MongoDB operation ***
			mongoCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			session := s.MongoSession.Copy()
			defer session.Close()
			c := session.DB("profile-db").C("hotels")

			hotelProf := new(pb.Hotel)
			mongoSpan, mongoCtx := tracer.StartSpanFromContext(mongoCtx, "mongo.query",
				tracer.Tag(ext.DBType, "mongo"),                                               // Specifies the database type as MongoDB
				tracer.Tag(ext.SpanType, "db"),                                                // Classifies this as a MongoDB span
				tracer.Tag(ext.DBInstance, "profile-db"),                                      // Specifies the database name
				tracer.Tag(ext.SpanKind, ext.SpanKindClient),                                  // Classifies the span as a client span
				tracer.Tag(ext.DBStatement, fmt.Sprintf("db.hotels.find({id: %q})", hotelId)), // Describes the database operation
				tracer.Tag(ext.ResourceName, "MongoDB: Find Hotel By ID"),                     // Human-readable resource name
			)
			defer mongoSpan.Finish()

			err := c.Find(bson.M{"id": hotelId}).One(&hotelProf)

			if err != nil {
				mongoSpan.SetTag(ext.Error, true)
				mongoSpan.SetTag("error.message", fmt.Sprintf("Failed to fetch hotel data: %v", err))
				log.Error().Msgf("Failed to get hotel data: %v", err)
				return
			}

			mutex.Lock()
			hotels = append(hotels, hotelProf)
			mutex.Unlock()

			// Cache result in Memcached
			profJson, _ := json.Marshal(hotelProf)
			s.MemcClient.Set(&memcache.Item{Key: hotelId, Value: profJson})
		}(hotelId)
	}
	wg.Wait()

	res.Hotels = hotels
	return res, nil
}
