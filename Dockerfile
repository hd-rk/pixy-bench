FROM golang:1.10.3-stretch

RUN apt-get update && apt-get -y install unzip && apt-get clean

# install protobuf
ENV PB_VER 3.1.0
ENV PB_URL https://github.com/google/protobuf/releases/download/v${PB_VER}/protoc-${PB_VER}-linux-x86_64.zip
RUN mkdir -p /tmp/protoc && \
    curl -L ${PB_URL} > /tmp/protoc/protoc.zip && \
    cd /tmp/protoc && \
    unzip protoc.zip && \
    cp /tmp/protoc/bin/protoc /usr/local/bin && \
    cp -R /tmp/protoc/include/* /usr/local/include && \
    chmod go+rx /usr/local/bin/protoc && \
    cd /tmp && \
    rm -r /tmp/protoc

# Get the source from GitHub
RUN go get google.golang.org/grpc && \
    go get github.com/golang/protobuf/protoc-gen-go && \
    go get github.com/Shopify/sarama && \
    go get google.golang.org/grpc/encoding/gzip && \
    go get cloud.google.com/go/pubsub

COPY lib     $GOPATH/src/pixy-bench/lib
COPY input   $GOPATH/src/pixy-bench/input
COPY main.go $GOPATH/src/pixy-bench

WORKDIR $GOPATH/src/pixy-bench/