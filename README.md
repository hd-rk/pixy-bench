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
go run main.go (mlisa-stream|mlisa-stream-p) {num_of_messages} (8|64|512)kb.txt {proxy_host:proxy_port} {kafka_topic}
```
