package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
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
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	// "reflect"
)

var (
	paramMap = map[string][]string{
		"pub-ctrl":       []string{"topic", "interval", "input"},
		"sub-ctrl":       []string{"server", "topic"},
		"multi-sub-ctrl": []string{"server", "startAt", "repeat"},
		"multi-pub-ctrl": []string{"startAt", "repeat", "interval", "input"},
		"default":        []string{"server", "topic", "repeat", "input"},
	}
	cert credentials.TransportCredentials
)

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
	var repeat, startAt, interval int
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
			log.Printf("repeating %d ", repeat)
		case "startAt":
			startAtStr := os.Getenv("START_AT")
			startAt, err = strconv.Atoi(startAtStr)
			if err != nil {
				log.Fatalf("invalid start-at %v", startAtStr)
			}
			log.Printf("starting at %d ", startAt)
		case "interval":
			intervalStr := os.Getenv("INTERVAL")
			interval, err = strconv.Atoi(intervalStr)
			if err != nil {
				log.Fatalf("invalid interval %v", intervalStr)
			}
			log.Printf("interval is %d ", interval)
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
	cert = credentials.NewTLS(&tls.Config{
		// InsecureSkipVerify: false,
		// ServerName:         strings.Split(server, ":")[0],
	})

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
		subscribeControlTopic(server, topic)
	case "pub-ctrl":
		publishControlTopic(topic, interval, data)
	case "multi-sub-ctrl":
		multipleSubscribeControlTopic(server, startAt, repeat)
	case "multi-pub-ctrl":
		multiplePublishControlTopic(interval, data, startAt, repeat)
	}
}

func multipleSubscribeControlTopic(address string, startAt int, numOfTopic int) {
	for i := 0; i < numOfTopic; i++ {
		go subscribeControlTopic(address, fmt.Sprintf("sz-%v", startAt+i))
	}

	select {
	case <-context.Background().Done():
	}
}

func multiplePublishControlTopic(interval int, data []byte, startAt int, numOfTopic int) {
	for i := 0; i < numOfTopic; i++ {
		go publishControlTopic(fmt.Sprintf("sz-%v", startAt+i), interval, data)
	}

	select {
	case <-context.Background().Done():
	}
}

func subscribeControlTopic(address string, topic string) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-mlisa-service-topic", "mlisa-control")
	ctx = metadata.AppendToOutgoingContext(ctx, "x-cluster-id", topic)

	client := mlisa.NewControlMessageProxyClient(conn)
	stream, err := client.StreamingPull(ctx)
	if err != nil {
		log.Fatalf("err in ctrl stream creation: %v", err)
	}

	rand.Seed(time.Now().Unix())
	statuses := []string{
		"200", // OK
		"501", // Not implemented
		"418", // I'm a teapot
	}

	i := 0
	for {
		message, err := stream.Recv()
		if err == nil {
			log.Printf("got message %v from topic %v", i, topic)
			log.Println(string(message.GetMessagePayloads()[0]))
			ackMsg := &mlisa.ControlMessagePullRequest{
				MessagePayloads: [][]byte{
					[]byte(fmt.Sprintf("status: %v, request: %v", statuses[rand.Intn(len(statuses))], string(message.GetMessagePayloads()[0]))),
				},
			}
			err = stream.Send(ackMsg)
			if err != nil {
				log.Printf("err sending sz ack %v", err)
			}
			i++
		} else {
			log.Fatalf("err in ctrl stream receive: %v", err)
		}
	}
}

func publishControlTopic(topic string, interval int, data []byte) {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "ruckussgdc-rsa-builder")
	if err != nil {
		log.Fatalf("cannot create pubsub client %v", err)
	}
	psTopic := client.Topic("mlisa-control.request.sz." + topic)
	defer psTopic.Stop()
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()
	msg := &pubsub.Message{Data: data}
	log.Printf("publishing messages to pubsub topic %v every %v seconds", psTopic.ID(), interval)
	for {
		select {
		case <-ticker.C:
			psTopic.Publish(ctx, msg)
		}
	}
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
		grpc.WithTransportCredentials(cert),
		// grpc.WithInsecure(),
		grpc.WithInitialWindowSize(int32(4*1024*1024)),
		grpc.WithWriteBufferSize(4*1024*1024))
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	duration := time.Duration(int64(0))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-mlisa-service-topic", topic, "x-mlisa-enable-ack", "true")

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
				log.Printf("got ack from server: %v %v", ack.GetBackend(), ack.GetBackendTopic())
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
