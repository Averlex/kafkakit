package kafka

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

// Publish sends the message to the given topic. Broker controller should be connected - returns error otherwise.
// If the topic does not exist, it is created automatically unless
// TopicCreateAttempts is 0.
//
// Any JSON-serializable value can be published. Byte slices are sent as raw
// payloads. A struct with exported Headers and Payload fields uses the envelope
// format: Payload is JSON encoded as the Kafka value, and JSON-tagged fields
// from the Headers struct become Kafka headers. Other message forms are
// published without Kafka headers.
//
// Example envelope message:
//
//	type HeadersType struct{
//		// Some serializable struct
//	}
//	type PayloadType struct{
//		// Some serializable struct
//	}
//	type Message struct{
//		Headers HeadersType
//		Payload PayloadType
//	}
func (b *Broker) Publish(ctx context.Context, topic string, msg any) error {
	result := b.PublishAsync(ctx, topic, msg)
	<-result.Done()

	return result.Err()
}

// PublishAsync begins publishing one message and returns its completion
// result. Encoding and submission may apply backpressure before this method
// returns. Done closes after Sarama reports the final delivery outcome.
func (b *Broker) PublishAsync(ctx context.Context, topic string, msg any) PublishResult {
	if b.controller == nil {
		return newCompletedPublishResult(
			fmt.Errorf("not connected to kafka, call Connect() first"),
			nil,
		)
	}

	if msg == nil {
		return newCompletedPublishResult(errors.New("no kafka message provided"), nil)
	}

	return b.sendBatchAsync(ctx, topic, []any{msg}, false)
}

// PublishBatch submits every element of a slice to the given topic as one
// application-facing batch. Sarama determines the actual broker request
// boundaries from the configured batching limits. Broker controller should be
// connected - returns error otherwise. An empty slice is a successful no-op.
// Any non-slice value results in an error.
//
// A partial failure is reported as a *PublishError holding one error per input
// element, which BatchErrors unpacks:
//
//	if err := broker.PublishBatch(ctx, topic, msgs); err != nil {
//		results, ok := BatchErrors(err)
//		if !ok {
//			// the call failed as a whole, nothing was published
//		}
//		for i, res := range results {
//			// res is nil when msgs[i] was published
//		}
//	}
func (b *Broker) PublishBatch(ctx context.Context, topic string, msgs any) error {
	result := b.PublishBatchAsync(ctx, topic, msgs)
	<-result.Done()

	return result.Err()
}

// PublishBatchAsync begins publishing every element of a slice and returns a
// completion result. All encodable messages are attempted even when another
// element fails. MessageErrors remains aligned with the original input order.
func (b *Broker) PublishBatchAsync(ctx context.Context, topic string, msgs any) BatchPublishResult {
	if b.controller == nil {
		return newCompletedPublishResult(
			fmt.Errorf("not connected to kafka, call Connect() first"),
			nil,
		)
	}

	if msgs == nil {
		return newCompletedPublishResult(errors.New("no kafka message batch provided"), nil)
	}

	batch, err := asBatch(msgs)
	if err != nil {
		return newCompletedPublishResult(err, nil)
	}

	return b.sendBatchAsync(ctx, topic, batch, true)
}

// sendBatchAsync converts and submits messages together, then completes the
// result with outcomes aligned to the original input.
func (b *Broker) sendBatchAsync(
	ctx context.Context,
	topic string,
	msgs []any,
	batch bool,
) *publishResult {
	result := newPendingPublishResult()
	results := make([]error, len(msgs))
	brokerMsgs := make([]BrokerMessage, 0, len(msgs))
	indexes := make([]int, 0, len(msgs))

	for i, msg := range msgs {
		if msg == nil {
			results[i] = errors.New("no kafka message provided")
			continue
		}

		brokerMsg, err := toBrokerMessage(msg)
		if err != nil {
			results[i] = fmt.Errorf("conversion error: %w", err)
			continue
		}

		brokerMsgs = append(brokerMsgs, *brokerMsg)
		indexes = append(indexes, i)
	}

	if len(brokerMsgs) == 0 {
		completeBrokerPublish(result, topic, results, nil, nil, nil, batch)

		return result
	}

	complete := func(published []error, err error) {
		completeBrokerPublish(result, topic, results, indexes, published, err, batch)
	}
	if controller, ok := b.controller.(asyncController); ok {
		controller.publishAsync(ctx, topic, brokerMsgs, complete)

		return result
	}

	controller := b.controller
	go func() {
		published, err := controller.Publish(ctx, topic, brokerMsgs)
		complete(published, err)
	}()

	return result
}

type asyncController interface {
	publishAsync(
		ctx context.Context,
		topic string,
		messages []BrokerMessage,
		complete func([]error, error),
	)
}

func completeBrokerPublish(
	result *publishResult,
	topic string,
	messageErrors []error,
	indexes []int,
	published []error,
	operationErr error,
	batch bool,
) {
	if operationErr != nil {
		result.complete(fmt.Errorf("publish to topic %s: %w", topic, operationErr), nil)

		return
	}

	if published != nil && len(published) != len(indexes) {
		result.complete(fmt.Errorf(
			"publish to topic %s: broker returned %d results for %d messages",
			topic,
			len(published),
			len(indexes),
		), nil)

		return
	}

	for i, publishErr := range published {
		if publishErr != nil {
			messageErrors[indexes[i]] = fmt.Errorf("publish to topic %s: %w", topic, publishErr)
		}
	}

	if batch {
		result.complete(newPublishError(messageErrors), messageErrors)

		return
	}

	result.complete(messageErrors[0], nil)
}

// asBatch converts a typed slice into individual messages.
func asBatch(msgs any) ([]any, error) {
	value := reflect.ValueOf(msgs)
	if value.Kind() != reflect.Slice {
		return nil, errors.New("kafka message batch must be a slice")
	}

	batch := make([]any, value.Len())
	for i := range batch {
		batch[i] = value.Index(i).Interface()
	}

	return batch, nil
}
