package reservation

import (
	// "encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/harlow/go-micro-services/registry"
	pb "github.com/harlow/go-micro-services/services/reservation/proto"
	"github.com/harlow/go-micro-services/tls"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/stats"
	ddgrpc "gopkg.in/DataDog/dd-trace-go.v1/contrib/google.golang.org/grpc"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	// "io/ioutil"
	"net"
	// "os"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/rs/zerolog/log"

	"strconv"
	"strings"
	"sync"
)

const name = "srv-reservation"

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

// Server implements the user service
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

	pb.RegisterReservationServer(srv, s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		log.Fatal().Msgf("failed to listen: %v", err)
	}

	log.Trace().Msgf("In reservation s.IpAddr = %s, port = %d", s.IpAddr, s.Port)

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

// MakeReservation makes a reservation based on given information
func (s *Server) MakeReservation(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	res := new(pb.Result)
	res.HotelId = make([]string, 0)

	// session, err := mgo.Dial("mongodb-reservation")
	// if err != nil {
	// 	panic(err)
	// }
	// defer session.Close()
	session := s.MongoSession.Copy()
	defer session.Close()

	c := session.DB("reservation-db").C("reservation")
	c1 := session.DB("reservation-db").C("number")

	inDate, _ := time.Parse(
		time.RFC3339,
		req.InDate+"T12:00:00+00:00")

	outDate, _ := time.Parse(
		time.RFC3339,
		req.OutDate+"T12:00:00+00:00")
	hotelId := req.HotelId[0]

	indate := inDate.String()[0:10]

	memc_date_num_map := make(map[string]int)

	for inDate.Before(outDate) {
		// Start a new date iteration span
		iterationSpan, ctx := tracer.StartSpanFromContext(ctx, "reservation.date_iteration")
		iterationSpan.SetTag("inDate", inDate.Format("2006-01-02"))
		defer iterationSpan.Finish()

		count := 0
		inDate = inDate.AddDate(0, 0, 1)
		outdate := inDate.String()[0:10]

		// Memcached Get for reservation count
		memcKey := hotelId + "_" + inDate.Format("2006-01-02") + "_" + outdate
		memcGetSpan, ctx := tracer.StartSpanFromContext(ctx, "memcached.get", tracer.ResourceName("GetReservationCount"))
		memcGetSpan.SetTag("cacheKey", memcKey)
		memcGetSpan.SetTag(ext.Component, "memcached")
		item, err := s.MemcClient.Get(memcKey)
		memcGetSpan.Finish()

		if err == nil {
			// Memcached hit
			count, _ = strconv.Atoi(string(item.Value))
			log.Trace().Msgf("Memcached hit %s = %d", memcKey, count)
			memc_date_num_map[memcKey] = count + int(req.RoomNumber)
		} else if err == memcache.ErrCacheMiss {
			// Memcached miss
			log.Trace().Msgf("Memcached miss")
			reserve := make([]reservation, 0)

			// MongoDB query for reservation with enhanced tagging
			mongoSpan, _ := tracer.StartSpanFromContext(ctx, "mongo.query",
				tracer.Tag(ext.DBType, "mongo"),              // Specifies the database type
				tracer.Tag(ext.SpanType, "db"),               // Classifies this as a database span
				tracer.Tag(ext.DBInstance, "reservation-db"), // Specifies the database name
				tracer.Tag(ext.SpanKind, ext.SpanKindClient), // Classifies the span as a client span
				tracer.Tag(ext.DBStatement, fmt.Sprintf( // Describes the query operation
					"db.reservation.find({hotelId: %s, inDate: %s, outDate: %s})",
					hotelId, indate, outdate,
				)),
				tracer.Tag(ext.ResourceName, "MongoDB: Find Reservations"), // A human-readable name for the operation
				tracer.Tag("hotel_id", hotelId),                            // Captures the specific hotel ID being queried
				tracer.Tag("reservation.in_date", indate),                  // Captures the start date of the reservation
				tracer.Tag("reservation.out_date", outdate),                // Captures the end date of the reservation
			)
			defer mongoSpan.Finish() // Ensure the span is always closed

			// Execute the query and handle any errors
			err := c.Find(&bson.M{"hotelId": hotelId, "inDate": indate, "outDate": outdate}).All(&reserve)
			if err != nil {
				mongoSpan.SetTag(ext.Error, true)              // Indicates an error occurred
				mongoSpan.SetTag("error.message", err.Error()) // Provides the error message for visibility
				log.Panic().Msgf("Tried to find hotelId [%v] from date [%v] to date [%v], but got error: %v", hotelId, indate, outdate, err)
			}

			// Add metadata after successful query execution
			mongoSpan.SetTag("reservations.count", len(reserve)) // Tag for the number of reservations found

			for _, r := range reserve {
				count += r.Number
			}
			memc_date_num_map[memcKey] = count + int(req.RoomNumber)
		} else {
			iterationSpan.SetTag(ext.Error, true)
			iterationSpan.SetTag("error.message", err.Error())
			log.Panic().Msgf("Tried to get memc_key [%v], but got memcached error = %s", memcKey, err)
		}

		// Memcached Get for hotel capacity
		memcCapKey := hotelId + "_cap"
		memcCapGetSpan, ctx := tracer.StartSpanFromContext(ctx, "memcached.get", tracer.ResourceName("GetHotelCapacity"))
		memcCapGetSpan.SetTag("cacheKey", memcCapKey)
		memcCapGetSpan.SetTag(ext.Component, "memcached")
		item, err = s.MemcClient.Get(memcCapKey)
		memcCapGetSpan.Finish()

		hotelCap := 0
		if err == nil {
			// Memcached hit
			hotelCap, _ = strconv.Atoi(string(item.Value))
			log.Trace().Msgf("Memcached hit %s = %d", memcCapKey, hotelCap)
		} else if err == memcache.ErrCacheMiss {
			// Memcached miss
			var num number
			mongoCapSpan, ctx := tracer.StartSpanFromContext(ctx, "mongo.query", tracer.ResourceName("FindHotelCapacity"))
			mongoCapSpan.SetTag(ext.DBInstance, "reservation-db")
			mongoCapSpan.SetTag(ext.DBStatement, fmt.Sprintf("Find capacity for hotelId=%s", hotelId))
			err = c1.Find(&bson.M{"hotelId": hotelId}).One(&num)
			mongoCapSpan.Finish()

			if err != nil {
				iterationSpan.SetTag(ext.Error, true)
				iterationSpan.SetTag("error.message", err.Error())
				log.Panic().Msgf("Tried to find hotelId [%v], but got error: %v", hotelId, err)
			}
			hotelCap = int(num.Number)

			// Memcached Set for hotel capacity
			memcCapSetSpan, ctx := tracer.StartSpanFromContext(ctx, "memcached.set", tracer.ResourceName("SetHotelCapacity"))
			memcCapSetSpan.SetTag("cacheKey", memcCapKey)
			memcCapSetSpan.SetTag(ext.Component, "memcached")
			s.MemcClient.Set(&memcache.Item{Key: memcCapKey, Value: []byte(strconv.Itoa(hotelCap))})
			memcCapSetSpan.Finish()
		} else {
			iterationSpan.SetTag(ext.Error, true)
			iterationSpan.SetTag("error.message", err.Error())
			log.Panic().Msgf("Tried to get memc_cap_key [%v], but got memcached error = %s", memcCapKey, err)
		}

		// Check capacity
		if count+int(req.RoomNumber) > hotelCap {
			return res, nil
		}
		indate = outdate
	}

	// Memcached Set for reservation counts
	for key, val := range memc_date_num_map {
		memcSetSpan, _ := tracer.StartSpanFromContext(ctx, "memcached.set", tracer.ResourceName("UpdateReservationCount"))
		memcSetSpan.SetTag("cacheKey", key)
		memcSetSpan.SetTag(ext.Component, "memcached")
		s.MemcClient.Set(&memcache.Item{Key: key, Value: []byte(strconv.Itoa(val))})
		memcSetSpan.Finish()
	}

	inDate, _ = time.Parse(
		time.RFC3339,
		req.InDate+"T12:00:00+00:00")

	indate = inDate.String()[0:10]

	for inDate.Before(outDate) {
		// Start a span for each reservation insertion
		span, _ := tracer.StartSpanFromContext(ctx, "mongo.insert", tracer.ResourceName("InsertReservation"))
		span.SetTag(ext.DBInstance, "reservation-db")
		span.SetTag(ext.DBStatement, "Insert reservation")
		span.SetTag("hotelId", hotelId)
		span.SetTag("customerName", req.CustomerName)
		span.SetTag("inDate", inDate.Format("2006-01-02"))
		span.SetTag("outDate", inDate.AddDate(0, 0, 1).Format("2006-01-02"))

		inDate = inDate.AddDate(0, 0, 1)
		outdate := inDate.String()[0:10]

		err := c.Insert(&reservation{
			HotelId:      hotelId,
			CustomerName: req.CustomerName,
			InDate:       indate,
			OutDate:      outdate,
			Number:       int(req.RoomNumber),
		})

		if err != nil {
			span.SetTag(ext.Error, true)
			span.SetTag("error.message", err.Error())
			log.Panic().Msgf("Tried to insert hotel [hotelId %v], but got error: %v", hotelId, err)
		}

		span.Finish() // Finish the span after each iteration
		indate = outdate
	}

	res.HotelId = append(res.HotelId, hotelId)

	return res, nil
}

// CheckAvailability checks if given information is available
func (s *Server) CheckAvailability(ctx context.Context, req *pb.Request) (*pb.Result, error) {
	res := new(pb.Result)
	res.HotelId = make([]string, 0)

	// session, err := mgo.Dial("mongodb-reservation")
	// if err != nil {
	// 	panic(err)
	// }
	// defer session.Close()
	session := s.MongoSession.Copy()
	defer session.Close()

	c1 := session.DB("reservation-db").C("number")

	hotelMemKeys := []string{}
	keysMap := make(map[string]struct{})
	resMap := make(map[string]bool)
	// cache capacity since it will not change
	for _, hotelId := range req.HotelId {
		hotelMemKeys = append(hotelMemKeys, hotelId+"_cap")
		resMap[hotelId] = true
		keysMap[hotelId+"_cap"] = struct{}{}
	}
	capMemSpan, ctx := tracer.StartSpanFromContext(ctx, "memcached.query", tracer.ResourceName("memcached_capacity_get_multi_number"))
	capMemSpan.SetTag(ext.Component, "memcached")
	capMemSpan.SetTag(ext.SpanType, "cache")
	capMemSpan.SetTag("span.kind", "client")
	cacheMemRes, err := s.MemcClient.GetMulti(hotelMemKeys)
	capMemSpan.Finish()
	misKeys := []string{}
	// gather cache miss key to query in mongodb
	if err == memcache.ErrCacheMiss {
		for key := range keysMap {
			if _, ok := cacheMemRes[key]; !ok {
				misKeys = append(misKeys, key)
			}
		}
	} else if err != nil {
		log.Panic().Msgf("Tried to get memc_cap_key [%v], but got memmcached error = %s", hotelMemKeys, err)
	}
	// store whole capacity result in cacheCap
	cacheCap := make(map[string]int)
	for k, v := range cacheMemRes {
		hotelCap, _ := strconv.Atoi(string(v.Value))
		cacheCap[k] = hotelCap
	}
	if len(misKeys) > 0 {
		queryMissKeys := []string{}
		for _, k := range misKeys {
			queryMissKeys = append(queryMissKeys, strings.Split(k, "_")[0])
		}
		nums := []number{}
		// MongoDB query for capacity with Datadog tracing
		capMongoSpan, _ := tracer.StartSpanFromContext(ctx, "mongo.query",
			tracer.ResourceName("MongoDB: Find Capacities"),                        // Human-readable operation name
			tracer.Tag(ext.DBType, "mongo"),                                        // Specifies the database type
			tracer.Tag(ext.SpanKind, ext.SpanKindClient),                           // Classifies the span as a client span
			tracer.Tag(ext.DBInstance, "reservation-db"),                           // Specifies the database name
			tracer.Tag(ext.DBStatement, "db.capacity.find({hotelId: {$in: ...}})"), // Describes the database query
			tracer.Tag("query.keys.count", len(queryMissKeys)),                     // Captures the number of keys being queried
		)
		defer capMongoSpan.Finish() // Ensure the span is always closed

		// Execute the query and handle any errors
		err = c1.Find(bson.M{"hotelId": bson.M{"$in": queryMissKeys}}).All(&nums)
		if err != nil {
			capMongoSpan.SetTag(ext.Error, true)              // Marks the span with an error
			capMongoSpan.SetTag("error.message", err.Error()) // Adds the error message for context
			log.Panic().Msgf("Tried to find hotelId [%v], but got error: %v", queryMissKeys, err)
		}

		// Add metadata after query execution
		capMongoSpan.SetTag("results.count", len(nums)) // Tags the count of results retrieved

		for _, num := range nums {
			cacheCap[num.HotelId] = num.Number
			// we don't care set successfully or not
			go s.MemcClient.Set(&memcache.Item{Key: num.HotelId + "_cap", Value: []byte(strconv.Itoa(num.Number))})
		}
	}

	reqCommand := []string{}
	queryMap := make(map[string]map[string]string)
	for _, hotelId := range req.HotelId {
		log.Trace().Msgf("reservation check hotel %s", hotelId)
		inDate, _ := time.Parse(
			time.RFC3339,
			req.InDate+"T12:00:00+00:00")
		outDate, _ := time.Parse(
			time.RFC3339,
			req.OutDate+"T12:00:00+00:00")
		for inDate.Before(outDate) {
			indate := inDate.String()[:10]
			inDate = inDate.AddDate(0, 0, 1)
			outDate := inDate.String()[:10]
			memcKey := hotelId + "_" + outDate + "_" + outDate
			reqCommand = append(reqCommand, memcKey)
			queryMap[memcKey] = map[string]string{
				"hotelId":   hotelId,
				"startDate": indate,
				"endDate":   outDate,
			}
		}
	}

	type taskRes struct {
		hotelId  string
		checkRes bool
	}
	reserveMemSpan, ctx := tracer.StartSpanFromContext(ctx, "memcached_reservation_get_multi", tracer.ResourceName("MakeReservation"))
	reserveMemSpan.SetTag(ext.Component, "memcached")
	reserveMemSpan.SetTag(ext.SpanType, "cache")

	ch := make(chan taskRes)
	reserveMemSpan.SetTag("span.kind", "client")
	// check capacity in memcached and mongodb
	if itemsMap, err := s.MemcClient.GetMulti(reqCommand); err != nil && err != memcache.ErrCacheMiss {
		reserveMemSpan.Finish()
		log.Panic().Msgf("Tried to get memc_key [%v], but got memmcached error = %s", reqCommand, err)
	} else {
		reserveMemSpan.Finish()
		// go through reservation count from memcached
		go func() {
			for k, v := range itemsMap {
				id := strings.Split(k, "_")[0]
				val, _ := strconv.Atoi(string(v.Value))
				var res bool
				if val+int(req.RoomNumber) <= cacheCap[id] {
					res = true
				}
				ch <- taskRes{
					hotelId:  id,
					checkRes: res,
				}
			}
			if err == nil {
				close(ch)
			}
		}()
		// use miss reservation to get data from mongo
		// rever string to indata and outdate
		if err == memcache.ErrCacheMiss {
			var wg sync.WaitGroup
			for k := range itemsMap {
				delete(queryMap, k)
			}
			wg.Add(len(queryMap))
			go func() {
				wg.Wait()
				close(ch)
			}()
			for command := range queryMap {
				go func(comm string) {
					defer wg.Done()
					reserve := []reservation{}
					tmpSess := s.MongoSession.Copy()
					defer tmpSess.Close()
					queryItem := queryMap[comm]
					c := tmpSess.DB("reservation-db").C("reservation")

					// MongoDB query tracing with Datadog
					reserveMongoSpan, ctx := tracer.StartSpanFromContext(ctx, "mongo.query",
						tracer.ResourceName("MongoDB: Find Reservation"), // Human-readable operation name
						tracer.Tag(ext.DBType, "mongo"),                  // Specifies the database type
						tracer.Tag(ext.SpanKind, ext.SpanKindClient),     // Classifies the span as a client span
						tracer.Tag(ext.DBInstance, "reservation-db"),     // Specifies the database name
						tracer.Tag(ext.DBStatement, fmt.Sprintf("db.reservation.find({hotelId: %s, inDate: %s, outDate: %s})",
							queryItem["hotelId"], queryItem["startDate"], queryItem["endDate"])), // Describes the query
					)
					defer reserveMongoSpan.Finish() // Ensure the span is closed

					// Execute the query
					err := c.Find(&bson.M{
						"hotelId": queryItem["hotelId"],
						"inDate":  queryItem["startDate"],
						"outDate": queryItem["endDate"],
					}).All(&reserve)

					// Handle errors
					if err != nil {
						reserveMongoSpan.SetTag(ext.Error, true)              // Marks the span as erroneous
						reserveMongoSpan.SetTag("error.message", err.Error()) // Adds the error message for context
						log.Panic().Msgf("Tried to find hotelId [%v] from date [%v] to date [%v], but got error: %v",
							queryItem["hotelId"], queryItem["startDate"], queryItem["endDate"], err)
					}

					// Add metadata after successful query execution
					reserveMongoSpan.SetTag("results.count", len(reserve)) // Captures the number of results returned

					var count int
					for _, r := range reserve {
						log.Trace().Msgf("reservation check reservation number = %d", queryItem["hotelId"])
						count += r.Number
					}

					// Memcached update tracing
					memcacheSpan, ctx := tracer.StartSpanFromContext(ctx, "memcached.set", tracer.ResourceName("UpdateReservationCache"))
					memcacheSpan.SetTag(ext.Component, "memcached")
					memcacheSpan.SetTag(ext.PeerService, "memcached-reservation")
					memcacheSpan.SetTag("hotelId", queryItem["hotelId"])
					memcacheSpan.SetTag("cacheKey", comm)
					memcacheSpan.SetTag("reservationCount", count)

					err = s.MemcClient.Set(&memcache.Item{Key: comm, Value: []byte(strconv.Itoa(count))})
					memcacheSpan.Finish()

					if err != nil {
						log.Warn().Msgf("Failed to update memcached for key [%s]: %v", comm, err)
					}

					var res bool
					if count+int(req.RoomNumber) <= cacheCap[queryItem["hotelId"]] {
						res = true
					}

					ch <- taskRes{
						hotelId:  queryItem["hotelId"],
						checkRes: res,
					}
				}(command)
			}

		}
	}

	for task := range ch {
		if !task.checkRes {
			resMap[task.hotelId] = false
		}
	}
	for k, v := range resMap {
		if v {
			res.HotelId = append(res.HotelId, k)
		}
	}

	return res, nil
}

type reservation struct {
	HotelId      string `bson:"hotelId"`
	CustomerName string `bson:"customerName"`
	InDate       string `bson:"inDate"`
	OutDate      string `bson:"outDate"`
	Number       int    `bson:"number"`
}

type number struct {
	HotelId string `bson:"hotelId"`
	Number  int    `bson:"numberOfRoom"`
}
