package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"pixy-bench/lib/mlisa"
	"pixy-bench/lib/pixy"
	"runtime"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/Shopify/sarama"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	// "reflect"
)

var paramMap = map[string][]string{
	"pub-ctrl": []string{"topic", "repeat", "input"},
	"sub-ctrl": []string{"server", "topic", "repeat"},
	"default":  []string{"server", "topic", "repeat", "input"},
}

//BM_TYPE= SERVER= TOPIC= REPEAT= INPUT= go run main.go
func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	bm_type := os.Getenv("BM_TYPE")
	log.Printf("benchmark type %s", bm_type)

	otherParams, ok := paramMap[bm_type]
	if !ok {
		otherParams = paramMap["default"]
	}

	var server, topic string
	var repeat int
	var data []byte
	var err error

	for _, param := range otherParams {
		switch param {
		case "server":
			server = os.Getenv("SERVER")
			log.Printf("server at %s", server)
		case "topic":
			topic = os.Getenv("TOPIC")
			log.Printf("topic is %s", topic)
		case "repeat":
			repeatStr := os.Getenv("REPEAT")
			repeat, err = strconv.Atoi(repeatStr)
			if err != nil {
				log.Fatalf("invalid repeat %v", repeatStr)
			}
			log.Printf("sending %d messages", repeat)
		case "input":
			filename := os.Getenv("INPUT")
			data, err = ioutil.ReadFile(fmt.Sprintf("./input/%s", filename))
			if err != nil {
				log.Fatalf("cannot read input file %v", filename)
			}
			log.Printf("message content in file %s", filename)
		}
	}

	log.Println()

	switch bm_type {
	case "pixy-grpc":
		bm_grpc(server, topic, data, repeat)
	case "pixy-rest":
		bm_rest(server, topic, data, repeat)
	case "sarama":
		bm_sarama(server, topic, data, repeat)
	case "mlisa-stream":
		bm_grpc_stream(server, topic, data, repeat)
	case "mlisa-stream-p":
		bm_parallel(server, topic, data, repeat)
	case "sub-ctrl":
		subscribeControlTopic(server, topic, repeat)
	case "pub-ctrl":
		publishControlTopic(topic, repeat, data)
	}
}

func subscribeControlTopic(address string, topic string, repeat int) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "mlisa-service-topic", "mlisa-control")

	client := mlisa.NewControlMessageProxyClient(conn)
	stream, err := client.StreamingPull(ctx)
	if err != nil {
		log.Fatalf("err in ctrl stream creation: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < repeat; i++ {
			message, err := stream.Recv()
			if err == nil {
				log.Printf("got message #%v", i)
				go func() {
					ackMsg := &mlisa.ControlMessagePullRequest{MessagePayloads: message.GetMessagePayloads()}
					err = stream.Send(ackMsg)
					if err != nil {
						log.Printf("err sending sz ack %v", err)
					}
				}()
			} else {
				log.Fatalf("err in ctrl stream receive: %v", err)
				return
			}
		}
	}()

	// msg := &mlisa.CtrlReq{Payload: &mlisa.CtrlReq_Init{Init: &mlisa.CtrlReq_InitPayload{ClusterId: topic}}}
	// err = stream.Send(msg)
	// if err != nil {
	// 	log.Fatalf("err in stream send: %v", err)
	// }
	<-done
}

func publishControlTopic(topic string, repeat int, data []byte) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "ruckussgdc-rsa-builder")
	if err != nil {
		log.Fatalf("cannot create pubsub client %v", err)
	}
	psTopic := client.Topic("mlisa-control.request.sz." + topic)
	log.Printf("publishing %d messages to pubsub topic %v", repeat, psTopic.ID())
	msg := &pubsub.Message{Data: data}
	for i := 0; i < repeat; i++ {
		psTopic.Publish(ctx, msg)
	}
	psTopic.Stop()
	log.Println("Done publishing")
}

func bm_grpc(address string, topic string, data []byte, repeat int) {

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	duration := time.Duration(int64(0))
	type runFn func() error
	var thisFn runFn

	pixyClient := pixy.NewKafkaPixyClient(conn)
	pixyMsg := &pixy.ProdRq{Topic: topic, KeyUndefined: true, Message: data, AsyncMode: true}
	thisFn = func() error {
		_, err := pixyClient.Produce(ctx, pixyMsg)
		return err
	}

	log.Printf("start pixy grpc bench")

	for i := 0; i < repeat; i++ {
		start := time.Now()
		err := thisFn()
		duration = duration + time.Since(start)
		if err != nil {
			log.Fatalf("err in request: %v", err)
		}
	}

	log.Printf("done grpc in: %fs", duration.Seconds())
}

func bm_parallel(address string, topic string, data []byte, repeat int) {
	num_stream := 4
	var wg sync.WaitGroup
	wg.Add(num_stream)
	log.Printf("start %d parallel grpc streams bench for %s", num_stream, "mlisa")
	start := time.Now()
	for range make([]int, num_stream) {
		go func() {
			bm_grpc_stream(address, topic, data, repeat)
			wg.Done()
		}()
	}
	wg.Wait()
	log.Printf("done paralle grpc stream in: %fs", time.Since(start).Seconds())
}

func bm_grpc_stream(address string, topic string, data []byte, repeat int) {
	conn, err := grpc.Dial(address,
		grpc.WithInsecure(),
		grpc.WithInitialWindowSize(int32(4*1024*1024)),
		grpc.WithWriteBufferSize(4*1024*1024))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	duration := time.Duration(int64(0))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "mlisa-service-topic", topic, "mlisa-enable-ack", "true")

	client := mlisa.NewDataMessageProxyClient(conn)
	stream, err := client.StreamingPush(ctx)
	if err != nil {
		log.Fatalf("err in stream get: %v", err)
	}

	waitc := make(chan struct{})
	go func() {
		for {
			acks, err := stream.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}
			if err != nil {
				log.Fatalf("err in stream receive: %v", err)
			}
			for _, ack := range acks.GetPushAcknowledgements() {
				log.Printf("got ack from server %v %v", ack.GetBackend(), ack.GetBackendTopic())
			}
		}
	}()

	msg := &mlisa.DataMessagePushRequest{MessagePayloads: [][]byte{data}}

	log.Printf("start grpc stream bench for %s", "mlisa")

	for i := 0; i < repeat; i++ {
		start := time.Now()
		err := stream.Send(msg)
		duration = duration + time.Since(start)
		if err != nil {
			log.Fatalf("err in stream send: %v", err)
		}
	}

	log.Printf("done grpc stream in: %fs", duration.Seconds())

	err = stream.CloseSend()
	if err != nil {
		log.Fatalf("err in stream close: %v", err)
	}
	<-waitc
}

func bm_rest(address string, topic string, data []byte, repeat int) {
	transport := &http.Transport{
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     30 * time.Second,
		DisableKeepAlives:   false,
	}

	client := &http.Client{Transport: transport}

	url := fmt.Sprintf("http://%s/topics/%s/messages", address, topic)

	log.Println("start pixy rest bench")
	duration := time.Duration(int64(0))

	for i := 0; i < repeat; i++ {
		start := time.Now()
		base64Data := make([]byte, base64.StdEncoding.EncodedLen(len(data)))
		base64.StdEncoding.Encode(base64Data, data)
		res, err := client.Post(url, "text/plain", bytes.NewReader(base64Data))
		duration = duration + time.Since(start)
		if err != nil {
			log.Fatalf("err in request: %v", err)
		}
		io.Copy(ioutil.Discard, res.Body)
		res.Body.Close()
	}

	log.Printf("done pixy rest in: %fs", duration.Seconds())
}

func bm_sarama(bootstrap string, topic string, data []byte, repeat int) {
	config := sarama.NewConfig()
	config.ClientID = "bench-client"
	config.Version = sarama.V1_1_0_0
	config.ChannelBufferSize = 4096
	config.Net.KeepAlive = 75 * time.Second
	config.Producer.MaxMessageBytes = 12 * 1000000
	config.Producer.Flush.Frequency = 2 * time.Second
	config.Producer.Flush.Bytes = 12 * 1000000
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = 500 * time.Millisecond
	producer, err := sarama.NewAsyncProducer([]string{bootstrap}, config)
	if err != nil {
		panic(err)
	}
	defer producer.Close()

	log.Println("start kafka/sarama bench")
	start := time.Now()

	go func() {
		for err := range producer.Errors() {
			log.Println("Failed to produce message", err)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		successes := 0
		for ; successes < repeat; successes++ {
			<-producer.Successes()
		}
		log.Printf("Received %d acks", successes)
		wg.Done()
	}()

	inputCh := producer.Input()
	for sent := 0; sent < repeat; sent++ {
		inputCh <- &sarama.ProducerMessage{
			Topic: topic,
			Key:   nil,
			Value: sarama.ByteEncoder(data),
		}
	}

	wg.Wait()

	log.Printf("done kafka/sarama in: %fs", time.Since(start).Seconds())
}
