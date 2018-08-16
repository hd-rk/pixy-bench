package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/Shopify/sarama"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
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
	// "reflect"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	bm_type := os.Args[1]

	repeat, err := strconv.Atoi(os.Args[2])
	if err != nil {
		panic(err)
	}
	log.Printf("sending %d messages", repeat)

	filename := os.Args[3]
	log.Printf("message content in file %s", filename)

	server := os.Args[4]
	log.Printf("server at %s", server)

	topic := os.Args[5]
	log.Printf("topic is %s", topic)

	data, err := ioutil.ReadFile(fmt.Sprintf("./input/%s", filename))
	if err != nil {
		panic(err)
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
	}
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	duration := time.Duration(int64(0))

	client := mlisa.NewProxyClient(conn)
	stream, err := client.ProduceData(ctx)
	if err != nil {
		log.Fatalf("err in stream get: %v", err)
	}

	waitc := make(chan struct{})
	go func() {
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}
			if err != nil {
				log.Fatalf("err in stream receive: %v", err)
			}
		}
	}()

	msg := &mlisa.DataMsg{Topic: topic, Message: data}

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