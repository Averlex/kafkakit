// Package main is a simple library demo.
package main

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"

	"github.com/Averlex/kafkakit"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := kafka.Config{
		DSN:            "<change>:<me>",
		GroupID:        "test_group_id",
		MaxBytes:       kafka.DefaultMaxBytes,
		AutoCommit:     kafka.DefaultAutoCommit,
		ConnectionTest: kafka.DefaultConnectionTest,
		Mode:           kafka.DefaultMode,
	}
	b, err := kafka.NewBroker(&cfg)
	if err != nil {
		log.Fatalf("initialize kafka client: %s", err.Error())
	}

	err = b.Connect(ctx)
	if err != nil {
		log.Fatalf("connect to kafka client: %s", err.Error())
	}
	defer b.Close(ctx)

	type Headers struct {
		MessageID string `json:"message_id"`
	}
	type Payload struct {
		Field1 string `json:"field1"`
		Field2 string `json:"field2"`
		Field3 int64  `json:"field3"`
	}
	type Message struct {
		Headers Headers
		Payload Payload
	}
	exampleMessage := Message{
		Headers: Headers{
			MessageID: uuid.New().String(),
		},
		Payload: Payload{
			Field1: "hello",
			Field2: "world",
			Field3: 42,
		},
	}

	err = b.Publish(ctx, "test-topic", &exampleMessage)
	if err != nil {
		log.Printf("publish to kafka client: %s", err.Error())
		return
	}
	log.Printf("message with id %s successfully sent", exampleMessage.Headers.MessageID)

	handler := func(_ context.Context, msg any, unmarshal func(any, any) error) error {
		var serializedMessage Message
		err := unmarshal(msg, &serializedMessage)
		if err != nil {
			log.Printf("discard malformed message: %s", err.Error())

			return nil
		}
		log.Printf("message %s received successfully: %v", serializedMessage.Headers.MessageID, serializedMessage.Payload)

		return nil
	}

	done, err := b.Subscribe(ctx, "test-topic", handler)
	if err != nil {
		log.Printf("subscribe to kafka client: %s", err.Error())
		return
	}

	time.Sleep(10 * time.Second)
	cancel()
	<-done
	log.Printf("successfully done!")
}
