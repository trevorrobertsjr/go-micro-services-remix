package frontend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/grpc"

	recommendation "github.com/harlow/go-micro-services/services/recommendation/proto"
	reservation "github.com/harlow/go-micro-services/services/reservation/proto"
	user "github.com/harlow/go-micro-services/services/user/proto"
	"github.com/rs/zerolog/log"

	"github.com/harlow/go-micro-services/dialer"
	"github.com/harlow/go-micro-services/registry"
	profile "github.com/harlow/go-micro-services/services/profile/proto"
	search "github.com/harlow/go-micro-services/services/search/proto"
	"github.com/harlow/go-micro-services/tls"
	httptrace "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

// Server implements frontend service
type Server struct {
	searchClient         search.SearchClient
	profileClient        profile.ProfileClient
	recommendationClient recommendation.RecommendationClient
	userClient           user.UserClient
	reservationClient    reservation.ReservationClient
	KnativeDns           string
	IpAddr               string
	Port                 int
	// Tracer               tracer.Tracer
	Registry *registry.Client
}

// Run the server
func (s *Server) Run(ctx context.Context) error {
	if s.Port == 0 {
		return fmt.Errorf("Server port must be set")
	}

	log.Info().Msg("Initializing gRPC clients...")
	if err := s.initSearchClient(ctx, "srv-search"); err != nil {
		return err
	}

	if err := s.initProfileClient(ctx, "srv-profile"); err != nil {
		return err
	}

	if err := s.initRecommendationClient(ctx, "srv-recommendation"); err != nil {
		return err
	}

	if err := s.initUserClient(ctx, "srv-user"); err != nil {
		return err
	}

	if err := s.initReservation(ctx, "srv-reservation"); err != nil {
		return err
	}
	log.Info().Msg("Successfully initialized gRPC clients")

	// Create HTTP mux with tracing support
	mux := httptrace.NewServeMux()
	mux.Handle("/", http.FileServer(http.Dir("services/frontend/static")))
	mux.Handle("/hotels", httptrace.WrapHandler(http.HandlerFunc(s.searchHandler), "frontend", "/hotels"))
	mux.Handle("/recommendations", httptrace.WrapHandler(http.HandlerFunc(s.recommendHandler), "frontend", "/recommendations"))
	mux.Handle("/user", httptrace.WrapHandler(http.HandlerFunc(s.userHandler), "frontend", "/user"))
	mux.Handle("/reservation", httptrace.WrapHandler(http.HandlerFunc(s.reservationHandler), "frontend", "/reservation"))

	// Configure and start the HTTP server
	log.Info().Msg("Starting HTTP server")
	tlsConfig := tls.GetHttpsOpt()
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.Port),
		Handler: mux,
	}

	if tlsConfig != nil {
		log.Info().Msg("Serving HTTPS")
		srv.TLSConfig = tlsConfig
		return srv.ListenAndServeTLS("x509/server_cert.pem", "x509/server_key.pem")
	} else {
		log.Info().Msg("Serving HTTP")
		return srv.ListenAndServe()
	}
}

// Updated gRPC initialization to use context
func (s *Server) initSearchClient(ctx context.Context, name string) error {
	conn, err := s.getGrpcConn(ctx, name)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.searchClient = search.NewSearchClient(conn)
	return nil
}

func (s *Server) initProfileClient(ctx context.Context, name string) error {
	conn, err := s.getGrpcConn(ctx, name)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.profileClient = profile.NewProfileClient(conn)
	return nil
}

func (s *Server) initRecommendationClient(ctx context.Context, name string) error {
	conn, err := s.getGrpcConn(ctx, name)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.recommendationClient = recommendation.NewRecommendationClient(conn)
	return nil
}

func (s *Server) initUserClient(ctx context.Context, name string) error {
	conn, err := s.getGrpcConn(ctx, name)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.userClient = user.NewUserClient(conn)
	return nil
}

func (s *Server) initReservation(ctx context.Context, name string) error {
	conn, err := s.getGrpcConn(ctx, name)
	if err != nil {
		return fmt.Errorf("dialer error: %v", err)
	}
	s.reservationClient = reservation.NewReservationClient(conn)
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
// 		// grpc.WithStatsHandler(&dialer.TracerStatsHandler{}), // Existing stats handler
// 		dialer.WithUnaryInterceptor(), // Add unary interceptor
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

	conn, err := dialer.Dial(ctx, target,
		dialer.WithUnaryInterceptor(), // Add unary interceptor
	)
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

// DEBUG
func (s *Server) searchHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx := r.Context()

	log.Trace().Msg("searchHandler invoked")

	// in/out dates from query params
	inDate, outDate := r.URL.Query().Get("inDate"), r.URL.Query().Get("outDate")
	if inDate == "" || outDate == "" {
		log.Debug().Msg("inDate or outDate parameter is missing")
		http.Error(w, "Please specify inDate/outDate params", http.StatusBadRequest)
		return
	}

	// lat/lon from query params
	sLat, sLon := r.URL.Query().Get("lat"), r.URL.Query().Get("lon")
	if sLat == "" || sLon == "" {
		log.Debug().Msg("lat or lon parameter is missing")
		http.Error(w, "Please specify location params", http.StatusBadRequest)
		return
	}

	Lat, _ := strconv.ParseFloat(sLat, 32)
	lat := float32(Lat)
	Lon, _ := strconv.ParseFloat(sLon, 32)
	lon := float32(Lon)

	log.Debug().Msgf("Parsed request parameters: lat=%v, lon=%v, inDate=%s, outDate=%s", lat, lon, inDate, outDate)

	// search for best hotels
	searchSpan, ctx := tracer.StartSpanFromContext(ctx, "grpc.call", tracer.ResourceName("search.Nearby"))
	searchSpan.SetTag(ext.Component, "grpc-client")
	searchSpan.SetTag(ext.SpanKind, ext.SpanKindClient)
	searchSpan.SetTag("grpc.method", "Nearby")
	defer searchSpan.Finish()

	searchResp, err := s.searchClient.Nearby(ctx, &search.NearbyRequest{
		Lat:     lat,
		Lon:     lon,
		InDate:  inDate,
		OutDate: outDate,
	})
	if err != nil {
		searchSpan.SetTag(ext.Error, true)
		searchSpan.SetTag("error.message", err.Error())
		log.Error().Err(err).Msg("Error fetching search results")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Debug().Msgf("Received search response with hotel IDs: %v", searchResp.HotelIds)

	// grab locale from query params or default to en
	locale := r.URL.Query().Get("locale")
	if locale == "" {
		locale = "en"
	}

	// check availability
	reservationResp, err := s.reservationClient.CheckAvailability(ctx, &reservation.Request{
		HotelId:    searchResp.HotelIds,
		InDate:     inDate,
		OutDate:    outDate,
		RoomNumber: 1,
	})
	if err != nil {
		log.Error().Err(err).Msg("Error checking availability")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Debug().Msgf("Availability response received for hotel IDs: %v", reservationResp.HotelId)

	// fetch hotel profiles
	profileResp, err := s.profileClient.GetProfiles(ctx, &profile.Request{
		HotelIds: reservationResp.HotelId,
		Locale:   locale,
	})
	if err != nil {
		log.Error().Err(err).Msg("Error fetching hotel profiles")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Debug().Msgf("Profile response received with hotels: %+v", profileResp.Hotels)

	json.NewEncoder(w).Encode(geoJSONResponse(profileResp.Hotels))
	log.Trace().Msg("searchHandler completed successfully")
}

func (s *Server) recommendHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	log.Trace().Msg("recommendHandler invoked")

	// Parse latitude and longitude
	sLat, sLon := r.URL.Query().Get("lat"), r.URL.Query().Get("lon")
	if sLat == "" || sLon == "" {
		http.Error(w, "Missing 'lat' or 'lon' query parameter", http.StatusBadRequest)
		return
	}
	Lat, _ := strconv.ParseFloat(sLat, 64)
	Lon, _ := strconv.ParseFloat(sLon, 64)

	// Parse 'require' parameter
	require := r.URL.Query().Get("require")
	if require != "dis" && require != "rate" && require != "price" {
		http.Error(w, "Invalid 'require' parameter. Use 'dis', 'rate', or 'price'.", http.StatusBadRequest)
		return
	}

	recommendSpan, ctx := tracer.StartSpanFromContext(ctx, "grpc.call", tracer.ResourceName("recommendation.GetRecommendations"))
	recommendSpan.SetTag(ext.Component, "grpc-client")
	recommendSpan.SetTag("require", require)
	defer recommendSpan.Finish()

	// Fetch recommendations
	recResp, err := s.recommendationClient.GetRecommendations(ctx, &recommendation.Request{
		Require: require,
		Lat:     Lat,
		Lon:     Lon,
	})
	if err != nil {
		recommendSpan.SetTag(ext.Error, true)
		recommendSpan.SetTag("error.message", err.Error())
		log.Error().Err(err).Msg("Error fetching recommendations")
		http.Error(w, "Failed to fetch recommendations", http.StatusInternalServerError)
		return
	}

	log.Debug().Msgf("Received recommendations: %v", recResp.HotelIds)

	// Fetch hotel profiles
	profileSpan, ctx := tracer.StartSpanFromContext(ctx, "grpc.call", tracer.ResourceName("profile.GetProfiles"))
	profileSpan.SetTag(ext.Component, "grpc-client")
	defer profileSpan.Finish()

	profileResp, err := s.profileClient.GetProfiles(ctx, &profile.Request{
		HotelIds: recResp.HotelIds,
		Locale:   r.URL.Query().Get("locale"),
	})
	if err != nil {
		profileSpan.SetTag(ext.Error, true)
		profileSpan.SetTag("error.message", err.Error())
		log.Error().Err(err).Msg("Error fetching hotel profiles")
		http.Error(w, "Failed to fetch hotel profiles", http.StatusInternalServerError)
		return
	}

	log.Debug().Msgf("Profile response: %+v", profileResp.Hotels)

	json.NewEncoder(w).Encode(geoJSONResponse(profileResp.Hotels))
}

func (s *Server) userHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	username, password := r.URL.Query().Get("username"), r.URL.Query().Get("password")
	if username == "" || password == "" {
		http.Error(w, "Missing 'username' or 'password' parameter", http.StatusBadRequest)
		return
	}

	log.Debug().Msgf("Checking user credentials for username: %s", username)

	userSpan, ctx := tracer.StartSpanFromContext(ctx, "grpc.call", tracer.ResourceName("user.CheckUser"))
	userSpan.SetTag(ext.Component, "grpc-client")
	userSpan.SetTag("username", username)
	defer userSpan.Finish()

	recResp, err := s.userClient.CheckUser(ctx, &user.Request{
		Username: username,
		Password: password,
	})
	if err != nil {
		userSpan.SetTag(ext.Error, true)
		userSpan.SetTag("error.message", err.Error())
		log.Error().Err(err).Msg("Error checking user credentials")
		http.Error(w, "Failed to validate user credentials", http.StatusInternalServerError)
		return
	}

	message := "Login successfully!"
	if !recResp.Correct {
		message = "Login failed. Please check your username and password."
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"message": message,
	})
}

func (s *Server) reservationHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	inDate, outDate := r.URL.Query().Get("inDate"), r.URL.Query().Get("outDate")
	if inDate == "" || outDate == "" || !checkDataFormat(inDate) || !checkDataFormat(outDate) {
		http.Error(w, "Invalid 'inDate' or 'outDate' format. Expected YYYY-MM-DD.", http.StatusBadRequest)
		return
	}

	hotelId := r.URL.Query().Get("hotelId")
	customerName := r.URL.Query().Get("customerName")
	if hotelId == "" || customerName == "" {
		http.Error(w, "Missing 'hotelId' or 'customerName' parameter", http.StatusBadRequest)
		return
	}

	username, password := r.URL.Query().Get("username"), r.URL.Query().Get("password")
	if username == "" || password == "" {
		http.Error(w, "Missing 'username' or 'password' parameter", http.StatusBadRequest)
		return
	}

	roomCount, _ := strconv.Atoi(r.URL.Query().Get("number"))

	log.Debug().Msgf("Making reservation for hotelId: %s, customer: %s", hotelId, customerName)

	userSpan, ctx := tracer.StartSpanFromContext(ctx, "grpc.call", tracer.ResourceName("user.CheckUser"))
	userSpan.SetTag(ext.Component, "grpc-client")
	userSpan.SetTag("username", username)
	defer userSpan.Finish()

	recResp, err := s.userClient.CheckUser(ctx, &user.Request{
		Username: username,
		Password: password,
	})
	if err != nil || !recResp.Correct {
		userSpan.SetTag(ext.Error, true)
		userSpan.SetTag("error.message", "Invalid credentials")
		log.Error().Msg("User validation failed")
		http.Error(w, "Invalid username or password", http.StatusUnauthorized)
		return
	}

	reservationSpan, ctx := tracer.StartSpanFromContext(ctx, "grpc.call", tracer.ResourceName("reservation.MakeReservation"))
	reservationSpan.SetTag(ext.Component, "grpc-client")
	reservationSpan.SetTag("hotelId", hotelId)
	defer reservationSpan.Finish()

	resResp, err := s.reservationClient.MakeReservation(ctx, &reservation.Request{
		CustomerName: customerName,
		HotelId:      []string{hotelId},
		InDate:       inDate,
		OutDate:      outDate,
		RoomNumber:   int32(roomCount),
	})
	if err != nil || len(resResp.HotelId) == 0 {
		reservationSpan.SetTag(ext.Error, true)
		reservationSpan.SetTag("error.message", "Failed to reserve")
		log.Error().Err(err).Msg("Reservation failed")
		http.Error(w, "Reservation failed. Rooms may be fully booked.", http.StatusConflict)
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"message": "Reservation successful!",
	})
}

// return a geoJSON response that allows google map to plot points directly on map
// https://developers.google.com/maps/documentation/javascript/datalayer#sample_geojson
func geoJSONResponse(hs []*profile.Hotel) map[string]interface{} {
	fs := []interface{}{}

	for _, h := range hs {
		fs = append(fs, map[string]interface{}{
			"type": "Feature",
			"id":   h.Id,
			"properties": map[string]string{
				"name":         h.Name,
				"phone_number": h.PhoneNumber,
			},
			"geometry": map[string]interface{}{
				"type": "Point",
				"coordinates": []float32{
					h.Address.Lon,
					h.Address.Lat,
				},
			},
		})
	}

	return map[string]interface{}{
		"type":     "FeatureCollection",
		"features": fs,
	}
}

func checkDataFormat(date string) bool {
	if len(date) != 10 {
		return false
	}
	for i := 0; i < 10; i++ {
		if i == 4 || i == 7 {
			if date[i] != '-' {
				return false
			}
		} else {
			if date[i] < '0' || date[i] > '9' {
				return false
			}
		}
	}
	return true
}
