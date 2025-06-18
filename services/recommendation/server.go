package recommendation

import (
	"context"
	"fmt"
	"math"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/hailocab/go-geoindex"
	"github.com/harlow/go-micro-services/registry"
	pb "github.com/harlow/go-micro-services/services/recommendation/proto"
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

const name = "srv-recommendation"

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

// Server implements the recommendation service
type Server struct {
	hotels       map[string]Hotel
	Port         int
	IpAddr       string
	MongoSession *mgo.Session
	Registry     *registry.Client
	uuid         string
}

// Run starts the server
func (s *Server) Run() error {
	if s.Port == 0 {
		return fmt.Errorf("server port must be set")
	}

	// *** Updated: Add context propagation and tracing to loadRecommendations ***
	s.hotels = loadRecommendations(s.MongoSession)

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
	pb.RegisterRecommendationServer(srv, s)

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

// GetRecommendations returns recommendations within a given requirement.
// *** Updated to include context propagation and enhanced tracing ***
func (s *Server) GetRecommendations(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "recommendation.get_recommendations", tracer.ResourceName("GetRecommendations"))
	defer span.Finish()

	span.SetTag("request.require", req.Require)
	span.SetTag("request.lat", req.Lat)
	span.SetTag("request.lon", req.Lon)

	res := new(pb.Result)
	require := req.Require

	if require == "dis" {
		p1 := &geoindex.GeoPoint{Plat: req.Lat, Plon: req.Lon}
		min := math.MaxFloat64
		for _, hotel := range s.hotels {
			tmp := float64(geoindex.Distance(p1, &geoindex.GeoPoint{Plat: hotel.HLat, Plon: hotel.HLon})) / 1000
			if tmp < min {
				min = tmp
			}
		}
		for _, hotel := range s.hotels {
			tmp := float64(geoindex.Distance(p1, &geoindex.GeoPoint{Plat: hotel.HLat, Plon: hotel.HLon})) / 1000
			if tmp == min {
				res.HotelIds = append(res.HotelIds, hotel.HId)
			}
		}
	} else if require == "rate" {
		max := 0.0
		for _, hotel := range s.hotels {
			if hotel.HRate > max {
				max = hotel.HRate
			}
		}
		for _, hotel := range s.hotels {
			if hotel.HRate == max {
				res.HotelIds = append(res.HotelIds, hotel.HId)
			}
		}
	} else if require == "price" {
		min := math.MaxFloat64
		for _, hotel := range s.hotels {
			if hotel.HPrice < min {
				min = hotel.HPrice
			}
		}
		for _, hotel := range s.hotels {
			if hotel.HPrice == min {
				res.HotelIds = append(res.HotelIds, hotel.HId)
			}
		}
	} else {
		log.Warn().Msgf("Invalid 'require' parameter: %v", require)
		span.SetTag(ext.Error, true)
	}

	span.SetTag("response.hotel_count", len(res.HotelIds))
	return res, nil
}

// loadRecommendations loads hotel recommendations from MongoDB.
func loadRecommendations(session *mgo.Session) map[string]Hotel {
	// span, ctx := tracer.StartSpanFromContext(ctx, "mongo.load_recommendations", tracer.Tag(ext.SpanType, "db"))
	// defer span.Finish()

	// span.SetTag(ext.DBInstance, "recommendation-db")
	// span.SetTag(ext.DBStatement, "Fetch all recommendations")

	s := session.Copy()
	defer s.Close()

	c := s.DB("recommendation-db").C("recommendation")

	var hotels []Hotel
	err := c.Find(bson.M{}).All(&hotels)
	if err != nil {
		// span.SetTag(ext.Error, err)
		log.Error().Msgf("Failed to get hotel data: %v", err)
	}

	profiles := make(map[string]Hotel)
	for _, hotel := range hotels {
		profiles[hotel.HId] = hotel
	}

	// span.SetTag("hotels.loaded", len(profiles))
	return profiles
}

// Hotel struct represents a hotel recommendation.
type Hotel struct {
	ID     bson.ObjectId `bson:"_id"`
	HId    string        `bson:"hotelId"`
	HLat   float64       `bson:"lat"`
	HLon   float64       `bson:"lon"`
	HRate  float64       `bson:"rate"`
	HPrice float64       `bson:"price"`
}
