# pixy-bench
Client program to benchmark Kafka proxy alternatives
## Install dependencies
```
go get google.golang.org/grpc \
       github.com/golang/protobuf/protoc-gen-go \
       github.com/Shopify/sarama \
       google.golang.org/grpc/encoding/gzip
```
## Run
```
BM_TYPE=mlisa-stream TOPIC=mlisa-data.test-topic REPEAT=100 INPUT=1kb.txt SERVER=messagehub.staging.mlisa.io:19091 go run main.go
```
