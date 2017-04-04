package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

var (
	topicFlag = flag.String("topic", "", "topic name")
	debugFlag = flag.Bool("debug", false, "debug mode")
)

func main() {
	flag.Parse()
	count := 0

	topic := *topicFlag
	brokers := []string{"kafka:9092"}

	if topic == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *debugFlag {
		logger := log.New(os.Stderr, "", log.LstdFlags)
		logger.SetOutput(os.Stderr)
		sarama.Logger = logger
	}

	cfg := sarama.NewConfig()
	cfg.ClientID = "kmc"
	cfg.Consumer.Return.Errors = true

	client, err := sarama.NewClient(brokers, cfg)
	check(err)
	defer client.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumer, err := sarama.NewConsumerFromClient(client)
	check(err)
	defer consumer.Close()

	partitions, err := client.Partitions(topic)
	check(err)

ProcessingLoop:
	for _, partition := range partitions {
		offsetNewest, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		check(err)

		offsetOldest, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
		check(err)

		if offsetNewest > offsetOldest {
			partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
			check(err)
			defer partitionConsumer.Close()

		ConsumerLoop:
			for {
				select {
				case msg := <-partitionConsumer.Messages():
					count++

					if msg.Offset%10000 == 0 {
						log.Printf("%d/%d\n", msg.Offset, offsetNewest)
					}

					if msg.Offset >= offsetNewest-1 {
						break ConsumerLoop
					}
				case <-signals:
					break ProcessingLoop
				}
			}
		}
	}

	defer fmt.Printf("COUNT: %d\n", count)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
