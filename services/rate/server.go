package rate

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/google/uuid"
	"github.com/harlow/go-micro-services/registry"
	pb "github.com/harlow/go-micro-services/services/rate/proto"
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

const name = "srv-rate"

// Define tracerStatsHandler for Datadog tracing.
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

	switch event := rpcStats.(type) {
	case *stats.InPayload:
		span.SetTag("event", "in_payload")
		span.SetTag("bytes_received", event.Length)
	case *stats.OutPayload:
		span.SetTag("event", "out_payload")
		span.SetTag("bytes_sent", event.Length)
	case *stats.End:
		if event.Error != nil {
			span.SetTag(ext.Error, event.Error)
		}
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

// Server implements the rate service
type Server struct {
	Port         int
	IpAddr       string
	MongoSession *mgo.Session
	Registry     *registry.Client
	MemcClient   *memcache.Client
	uuid         string
}

// Run starts the server
func (s *Server) Run() error {
	if s.Port == 0 {
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
		grpc.UnaryInterceptor(ddgrpc.UnaryServerInterceptor()),   // Datadog unary interceptor
		grpc.StreamInterceptor(ddgrpc.StreamServerInterceptor()), // Datadog stream interceptor
	}

	if tlsopt := tls.GetServerOpt(); tlsopt != nil {
		opts = append(opts, tlsopt)
	}

	srv := grpc.NewServer(opts...)
	pb.RegisterRateServer(srv, s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		log.Fatal().Msgf("Failed to listen: %v", err)
	}

	// Construct service DNS address without the prefix
	namespace := "test-hotel-reservation"                                     // Replace with your namespace
	serviceDNS := fmt.Sprintf("%s.%s.svc.cluster.local", name[4:], namespace) // Strip "srv-" from `name`

	log.Info().Msgf("Registering service [name: %s, id: %s, address: %s, port: %d]", name, s.uuid, serviceDNS, s.Port)
	err = s.Registry.Register(name, s.uuid, serviceDNS, s.Port)
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

// GetRates gets rates for hotels for a specific date range.
// *** Updated to include context propagation ***
func (s *Server) GetRates(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "rate.GetRates")
	defer span.Finish()

	log.Trace().Msg("In GetRates")

	res := new(pb.Result)
	ratePlans := make(RatePlans, 0)

	hotelIds := []string{}
	rateMap := make(map[string]struct{})
	for _, hotelID := range req.HotelIds {
		hotelIds = append(hotelIds, hotelID)
		rateMap[hotelID] = struct{}{}
	}

	// *** Updated: Added timeout for Memcached operation ***
	memCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	// *** Updated: Added tracing for Memcached ***
	memSpan, memCtx := tracer.StartSpanFromContext(memCtx, "memcached.get_multi_rate", tracer.Tag(ext.SpanType, "cache"))
	memSpan.SetTag(ext.Component, "memcached")
	memSpan.SetTag(ext.PeerService, "memcached-rate")

	resMap, err := s.MemcClient.GetMulti(hotelIds) // Memcached does not support context directly
	memSpan.Finish()

	if err != nil && err != memcache.ErrCacheMiss {
		memSpan.SetTag(ext.Error, err)
		log.Panic().Msgf("Memcached error while trying to get hotel [id: %v]: %v", hotelIds, err)
	}

	var wg sync.WaitGroup
	var mutex sync.Mutex
	for hotelId, item := range resMap {
		rateStrs := strings.Split(string(item.Value), "\n")
		for _, rateStr := range rateStrs {
			if len(rateStr) != 0 {
				rateP := new(pb.RatePlan)
				json.Unmarshal([]byte(rateStr), rateP)
				ratePlans = append(ratePlans, rateP)
			}
		}
		delete(rateMap, hotelId)
	}

	wg.Add(len(rateMap))
	for hotelId := range rateMap {
		go func(id string) {
			defer wg.Done()

			// *** Updated: Added timeout for MongoDB operation ***
			mongoCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			session := s.MongoSession.Copy()
			defer session.Close()
			c := session.DB("rate-db").C("inventory")

			tmpRatePlans := make(RatePlans, 0)
			// *** Updated: Added tracing for MongoDB query ***
			// Start a new span for MongoDB query with enhanced tagging
			mongoSpan, mongoCtx := tracer.StartSpanFromContext(mongoCtx, "mongo.query",
				tracer.Tag(ext.DBType, "mongo"),                           // Specifies the database type
				tracer.Tag(ext.SpanType, "db"),                            // Classifies this as a database span
				tracer.Tag(ext.DBInstance, "rate-db"),                     // Specifies the database name
				tracer.Tag(ext.SpanKind, ext.SpanKindClient),              // Classifies the span as a client span
				tracer.Tag(ext.DBStatement, "db.rate.find({hotelId: ?})"), // Describes the query operation
				tracer.Tag(ext.ResourceName, "MongoDB: Find Rate Plans"),  // A human-readable name for the operation
				tracer.Tag("hotel_id", id),                                // Adds the specific hotel ID being queried
			)
			defer mongoSpan.Finish() // Ensure the span is always closed

			// Execute the query and handle any errors
			err := c.Find(&bson.M{"hotelId": id}).All(&tmpRatePlans) // `mgo.v2` does not directly support context
			if err != nil {
				mongoSpan.SetTag(ext.Error, true)              // Indicates an error occurred
				mongoSpan.SetTag("error.message", err.Error()) // Provides the error message for visibility
				log.Panic().Msgf("Failed to find rates for hotel ID [%v]: %v", id, err)
			}

			// Additional metadata after the query execution
			mongoSpan.SetTag("rate_plans.count", len(tmpRatePlans)) // Tag for the number of rate plans retrieved

			memcStr := ""
			for _, r := range tmpRatePlans {
				mutex.Lock()
				ratePlans = append(ratePlans, r)
				mutex.Unlock()
				rateJson, _ := json.Marshal(r)
				memcStr += string(rateJson) + "\n"
			}
			s.MemcClient.Set(&memcache.Item{Key: id, Value: []byte(memcStr)})
		}(hotelId)
	}
	wg.Wait()

	sort.Sort(ratePlans)
	res.RatePlans = ratePlans

	return res, nil
}

// RatePlans implements sorting for rate plans
type RatePlans []*pb.RatePlan

func (r RatePlans) Len() int {
	return len(r)
}

func (r RatePlans) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r RatePlans) Less(i, j int) bool {
	return r[i].RoomType.TotalRate > r[j].RoomType.TotalRate
}
