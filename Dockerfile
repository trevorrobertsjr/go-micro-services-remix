FROM golang:1.23

RUN git config --global http.sslverify false
COPY . /go/src/github.com/harlow/go-micro-services
WORKDIR /go/src/github.com/harlow/go-micro-services
# RUN go get gopkg.in/mgo.v2
# RUN go get github.com/bradfitz/gomemcache/memcache
# RUN go get github.com/google/uuid
# RUN go get gopkg.in/DataDog/dd-trace-go.v1
# RUN go mod init
RUN go mod init github.com/harlow/go-micro-services || true
RUN go mod tidy



# use local modified github.com/hailocab/go-geoindex instead of upstream one
COPY ./vendor/github.com/hailocab/go-geoindex /go/src/github.com/harlow/go-micro-services/vendor/github.com/hailocab/go-geoindex
RUN go mod vendor

RUN go install -ldflags="-s -w" ./cmd/...
