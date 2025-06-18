module github.com/harlow/go-micro-services

go 1.23

require (
	github.com/DataDog/datadog-api-client-go/v2 v2.34.0
	github.com/bradfitz/gomemcache v0.0.0-20230905024940-24af94b03874
	github.com/golang/protobuf v1.5.3
	github.com/google/uuid v1.6.0
	github.com/grpc-ecosystem/grpc-opentracing v0.0.0-20171214222146-0e7658f8ee99
	github.com/hailocab/go-geoindex v0.0.0-20160127134810-64631bfe9711
	github.com/hashicorp/consul v1.0.6
	github.com/olivere/grpc v1.0.0
	github.com/opentracing-contrib/go-stdlib v0.0.0-20180308002341-f6b9967a3c69
	github.com/opentracing/opentracing-go v1.0.2
	github.com/rs/zerolog v1.33.0
	github.com/uber/jaeger-client-go v2.11.2+incompatible
	golang.org/x/net v0.17.0
	google.golang.org/grpc v1.10.0
	gopkg.in/mgo.v2 v2.0.0-20190816093944-a6b53ec6cb22
)

require (
	github.com/DataDog/zstd v1.5.2 // indirect
	github.com/apache/thrift v0.0.0-20161221203622-b2a4d4ae21c7 // indirect
	github.com/armon/go-metrics v0.0.0-20180917152333-f0300d1749da // indirect
	github.com/codahale/hdrhistogram v0.9.0 // indirect
	github.com/goccy/go-json v0.10.2 // indirect
	github.com/golang/glog v1.2.3 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.0 // indirect
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-rootcerts v0.0.0-20160503143440-6bb64b370b90 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/memberlist v0.5.1 // indirect
	github.com/hashicorp/serf v0.8.1 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/mitchellh/go-homedir v0.0.0-20161203194507-b8bc1bf76747 // indirect
	github.com/mitchellh/go-testing-interface v1.14.1 // indirect
	github.com/pascaldekloe/goe v0.1.1 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	github.com/trevorrobertsjr/datadogwriter v0.1.3 // indirect
	github.com/uber-go/atomic v0.0.0-00010101000000-000000000000 // indirect
	github.com/uber/jaeger-lib v1.4.0 // indirect
	golang.org/x/oauth2 v0.10.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20180306020942-df60624c1e9b // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/yaml.v2 v2.2.5 // indirect
)

replace (
	github.com/armon/go-metrics => github.com/hashicorp/go-metrics v0.5.3
	github.com/uber-go/atomic => go.uber.org/atomic v1.11.0
)
