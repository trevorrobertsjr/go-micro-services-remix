package dialer

import (
	"context"
	"fmt"
	"time"

	"github.com/harlow/go-micro-services/tls"
	"github.com/hashicorp/consul/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/stats"
	ddgrpc "gopkg.in/DataDog/dd-trace-go.v1/contrib/google.golang.org/grpc"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

// consulResolverBuilder handles building resolvers for Consul service discovery.
type consulResolverBuilder struct {
	client *api.Client
}

// Build constructs the resolver for the given target.
func (b *consulResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &consulResolver{
		client: b.client,
		target: target.Endpoint(),
		cc:     cc,
		stopCh: make(chan struct{}),
	}
	go r.watchAgent()
	fmt.Printf("Consul resolver created for target: %s\n", target.Endpoint())
	return r, nil
}

// Scheme returns the scheme supported by this resolver builder.
func (b *consulResolverBuilder) Scheme() string {
	return "consul"
}

// consulResolver watches for updates to the Consul Agent services and updates the gRPC resolver state.
type consulResolver struct {
	client *api.Client
	target string
	cc     resolver.ClientConn
	stopCh chan struct{}
}

func (r *consulResolver) watchAgent() {
	normalizedTarget := r.target
	if len(normalizedTarget) > 10 && normalizedTarget[:10] == "consul:///" {
		normalizedTarget = normalizedTarget[10:]
	}
	for {
		select {
		case <-r.stopCh:
			return
		default:
			services, err := r.client.Agent().Services()
			if err != nil {
				fmt.Printf("[%s] Failed to resolve services for target %s: %v\n",
					time.Now().Format("2006-01-02 15:04:05"), r.target, err)
				time.Sleep(5 * time.Second)
				continue
			}

			var addresses []resolver.Address
			for _, service := range services {
				if service.Service == normalizedTarget {
					addresses = append(addresses, resolver.Address{
						Addr: fmt.Sprintf("%s:%d", service.Address, service.Port),
					})
					lastItem := addresses[len(addresses)-1].Addr
					fmt.Printf("Added service %s for target %s with address %s", service.Service, r.target, lastItem)
				}
			}

			r.cc.UpdateState(resolver.State{Addresses: addresses})
			time.Sleep(5 * time.Second)
		}
	}
}

func (r *consulResolver) ResolveNow(_ resolver.ResolveNowOptions) {}
func (r *consulResolver) Close()                                  { close(r.stopCh) }

// tracerStatsHandler implements gRPC stats.Handler for Datadog tracing.
type TracerStatsHandler struct{}

func (t *TracerStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	span, ctx := tracer.StartSpanFromContext(ctx, info.FullMethodName, tracer.Tag(ext.SpanType, "rpc"))
	return tracer.ContextWithSpan(ctx, span)
}

func (t *TracerStatsHandler) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {
	span, ok := tracer.SpanFromContext(ctx)
	if !ok {
		return
	}
	switch statsEvent := rpcStats.(type) {
	case *stats.InPayload:
		span.SetTag("bytes_received", statsEvent.Length)
	case *stats.OutPayload:
		span.SetTag("bytes_sent", statsEvent.Length)
	case *stats.End:
		if statsEvent.Error != nil {
			span.SetTag(ext.Error, statsEvent.Error)
		}
	}
	span.Finish()
}

func (t *TracerStatsHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (t *TracerStatsHandler) HandleConn(context.Context, stats.ConnStats) {}

// WithTracer enables Datadog tracing for gRPC calls.
func WithTracer() DialOption {
	return func(name string) (grpc.DialOption, error) {
		return grpc.WithStatsHandler(&TracerStatsHandler{}), nil
	}
}

// WithUnaryInterceptor enables the Datadog unary interceptor.
func WithUnaryInterceptor() DialOption {
	return func(name string) (grpc.DialOption, error) {
		// Use the Datadog-provided interceptor
		return grpc.WithUnaryInterceptor(ddgrpc.UnaryClientInterceptor(ddgrpc.WithServiceName(name))), nil
	}
}

// DialOption allows optional configurations for gRPC Dial.
type DialOption func(target string) (grpc.DialOption, error)

// Dial establishes a gRPC connection with optional configurations.
// func Dial(ctx context.Context, target string, customOpt grpc.DialOption, opts ...DialOption) (*grpc.ClientConn, error) {
// 	consulAddr := "consul.test-hotel-reservation.svc.cluster.local:8500"
// 	dialopts := []grpc.DialOption{}

// 	// Process DialOptions
// 	for _, opt := range opts {
// 		dialOption, err := opt(target)
// 		if err != nil {
// 			return nil, fmt.Errorf("failed to process DialOption: %v", err)
// 		}
// 		dialopts = append(dialopts, dialOption)
// 	}

// 	// Include the custom option
// 	if customOpt != nil {
// 		dialopts = append(dialopts, customOpt)
// 	}

// 	fmt.Printf("Dialing Consul target [%s] at address [%s]\n", target, consulAddr)

// 	// Add default gRPC options
// 	dialopts = append(dialopts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
// 		Timeout:             120 * time.Second,
// 		PermitWithoutStream: true,
// 	}))
// 	dialopts = append(dialopts, grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`))

// 	// Add TLS or insecure option
// 	if tlsopt := tls.GetDialOpt(); tlsopt != nil {
// 		dialopts = append(dialopts, tlsopt)
// 	} else {
// 		dialopts = append(dialopts, grpc.WithInsecure())
// 	}

// 	// Register Consul resolver
// 	resolver.Register(&consulResolverBuilder{
// 		client: getConsulClient(consulAddr),
// 	})

// 	// Use context when dialing
// 	conn, err := grpc.DialContext(ctx, fmt.Sprintf("consul:///%s", target), dialopts...)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to connect to target %s: %v", target, err)
// 	}

// 	fmt.Printf("Successfully connected to target [%s] via Consul address [%s]\n", target, consulAddr)
// 	return conn, nil
// }

func Dial(ctx context.Context, target string, opts ...DialOption) (*grpc.ClientConn, error) {
	consulAddr := "consul.test-hotel-reservation.svc.cluster.local:8500"
	dialopts := []grpc.DialOption{}

	// Process custom DialOptions and add them to the dial options
	for _, opt := range opts {
		grpcOpt, err := opt(target)
		if err != nil {
			return nil, fmt.Errorf("failed to process DialOption: %v", err)
		}
		dialopts = append(dialopts, grpcOpt)
	}

	fmt.Printf("Dialing Consul target [%s] at address [%s]\n", target, consulAddr)

	// Add default gRPC options
	dialopts = append(dialopts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Timeout:             120 * time.Second,
		PermitWithoutStream: true,
	}))
	dialopts = append(dialopts, grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`))

	// Add TLS or insecure option
	if tlsopt := tls.GetDialOpt(); tlsopt != nil {
		dialopts = append(dialopts, tlsopt)
	} else {
		dialopts = append(dialopts, grpc.WithInsecure())
	}

	// Register Consul resolver
	resolver.Register(&consulResolverBuilder{
		client: getConsulClient(consulAddr),
	})

	// Use context when dialing
	conn, err := grpc.DialContext(ctx, fmt.Sprintf("consul:///%s", target), dialopts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to target %s: %v", target, err)
	}

	fmt.Printf("Successfully connected to target [%s] via Consul address [%s]\n", target, consulAddr)
	return conn, nil
}

func getConsulClient(addr string) *api.Client {
	cfg := api.DefaultConfig()
	cfg.Address = addr

	client, err := api.NewClient(cfg)
	if err != nil {
		panic(fmt.Sprintf("failed to create Consul client: %v at address %s", err, addr))
	}
	return client
}
