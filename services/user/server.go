package user

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"time"

	"github.com/google/uuid"
	"github.com/harlow/go-micro-services/registry"
	pb "github.com/harlow/go-micro-services/services/user/proto"
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

const name = "srv-user"

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

// Server implements the user service
type Server struct {
	users        map[string]string
	Registry     *registry.Client
	Port         int
	IpAddr       string
	MongoSession *mgo.Session
	uuid         string
}

// Run starts the server
func (s *Server) Run() error {
	if s.Port == 0 {
		return fmt.Errorf("server port must be set")
	}

	s.users = loadUsers(s.MongoSession)

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

	pb.RegisterUserServer(srv, s)

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
		return fmt.Errorf("failed register: %v", err)
	}
	log.Info().Msg("Successfully registered in consul")

	return srv.Serve(lis)
}

// Shutdown cleans up any processes
func (s *Server) Shutdown() {
	s.Registry.Deregister(s.uuid)
}

// CheckUser returns whether the username and password are correct.
func (s *Server) CheckUser(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	span, ctx := tracer.StartSpanFromContext(ctx, "user.CheckUser", tracer.ResourceName("CheckUser"))
	defer span.Finish()

	res := new(pb.Result)

	log.Trace().Msg("CheckUser")

	sum := sha256.Sum256([]byte(req.Password))
	pass := fmt.Sprintf("%x", sum)

	res.Correct = false
	if truePass, found := s.users[req.Username]; found {
		res.Correct = pass == truePass
		if !res.Correct {
			span.SetTag(ext.Error, true)
		}
	} else {
		log.Warn().Msgf("User not found: %s", req.Username)
		span.SetTag(ext.Error, true)
	}

	log.Trace().Msgf("CheckUser result: %v", res.Correct)

	return res, nil
}

// loadUsers loads hotel users from MongoDB.
// *** Updated to accept context and add tracing ***
func loadUsers(session *mgo.Session) map[string]string {
	// span, ctx := tracer.StartSpanFromContext(ctx, "mongo.load_users", tracer.Tag(ext.SpanType, "db"))
	// defer span.Finish()

	// span.SetTag(ext.DBInstance, "user-db")
	// span.SetTag(ext.DBStatement, "Fetch all users")

	s := session.Copy()
	defer s.Close()
	c := s.DB("user-db").C("user")

	var users []User
	err := c.Find(bson.M{}).All(&users)
	if err != nil {
		// span.SetTag(ext.Error, err)
		log.Error().Msgf("Failed to get users data: %v", err)
	}

	res := make(map[string]string)
	for _, user := range users {
		res[user.Username] = user.Password
	}

	// span.SetTag("users.loaded", len(res))
	log.Trace().Msg("Done loading users")

	return res
}

// User struct represents a user in the system.
type User struct {
	Username string `bson:"username"`
	Password string `bson:"password"`
}
