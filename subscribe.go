package kafka

import (
	"context"
	"errors"
	"fmt"
)

// Subscribe subscribes to the given topic and processes incoming messages using the provided handler.
// The broker must be connected via Connect() before subscribing.
// Missing topics are never created by subscribing.
//
// On success, returns a channel that is closed when message processing stops (e.g., due to context cancellation
// or Unsubscribe), and a nil error. On failure, returns a closed channel and an error.
//
// The handler receives each message as an opaque `any` value, which is actually a pointer to
// the library's broker message structure (*BrokerMessage):
//
//	type BrokerMessage struct {
//		Headers map[string][]byte  // raw header values as byte slices
//		Payload []byte             // raw message payload as byte slice
//	}
//
// A built-in unmarshal helper (FromBrokerMessage) is provided to simplify
// deserialization. Its target may be a pointer to any JSON-decodable value. A
// byte slice receives the raw payload. Envelope structs with exported Headers
// and Payload fields decode Kafka headers and payload separately.
//
// Example envelope message type for unmarshaling:
//
//	type HeadersType struct {
//		CorrelationID string `json:"correlation_id"`
//		Token      	  []byte `json:"token"`
//	}
//	type PayloadType struct {
//		UserID string `json:"user_id"`
//		Action string `json:"action"`
//	}
//	type Message struct {
//		Headers HeadersType
//		Payload PayloadType
//	}
//
// Example handler using the built-in unmarshaler:
//
//	func handler(ctx context.Context, msg any, unmarshal func(any, any) error) error {
//		var m Message
//		if err := unmarshal(msg, &m); err != nil {
//			log.Printf("discard malformed message: %v", err)
//			return nil
//		}
//		// process m.Headers and m.Payload
//		return nil
//	}
//
// Using FromBrokerMessage is optional. A handler may type-assert *BrokerMessage
// to inspect the raw broker message directly.
//
// A nil handler result acknowledges the message. An error negatively
// acknowledges it; the same message is delivered again after NackBackoff and
// its partition does not advance. To stop retrying, the service must handle the
// terminal failure itself and return nil. Malformed or undecryptable data is
// normally terminal, while transient dependency errors are normally returned.
func (b *Broker) Subscribe(
	ctx context.Context,
	topic string,
	handler func(ctx context.Context, msg any, unmarshal func(any, any) error) error,
) (<-chan struct{}, error) {
	closedDone := make(chan struct{}) // To prevent reading from a nil-channel.
	close(closedDone)

	if handler == nil {
		return closedDone, errors.New("no handler received")
	}

	b.mu.Lock()
	if b.controller == nil {
		b.mu.Unlock()
		return closedDone, errors.New("not connected to kafka, call Connect() first")
	}

	if _, exists := b.subs[topic]; exists {
		b.mu.Unlock()
		return closedDone, fmt.Errorf("already subscribed to topic %s", topic)
	}

	subCtx, cancel := context.WithCancel(ctx)
	sub, err := b.controller.Subscribe(subCtx, topic)
	if err != nil {
		cancel()
		b.mu.Unlock()
		return closedDone, fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}

	activeSubscription := &subscription{sub: sub, cancel: cancel}
	b.subs[topic] = activeSubscription

	done := make(chan struct{})
	// Start the message processor under the mutex to prevent a possible race with Close().
	go b.processMessages(subCtx, topic, activeSubscription, handler, done)

	b.mu.Unlock()
	return done, nil
}

// Unsubscribe cancels the topic subscription.
func (b *Broker) Unsubscribe(ctx context.Context, topic string) error {
	b.mu.Lock()
	s, exists := b.subs[topic]
	if !exists {
		b.mu.Unlock()
		return fmt.Errorf("no subscription found for topic %s", topic)
	}
	delete(b.subs, topic)
	b.mu.Unlock()

	s.cancel()
	s.sub.Cancel(ctx)
	return nil
}

// processMessages processes messages extracted from topic.
func (b *Broker) processMessages(
	ctx context.Context,
	topic string,
	activeSubscription *subscription,
	handler func(ctx context.Context, msg any, unmarshal func(any, any) error) error,
	done chan<- struct{},
) {
	defer func() {
		b.mu.Lock()
		if b.subs[topic] == activeSubscription {
			delete(b.subs, topic)
		}
		b.mu.Unlock()
		close(done)
	}()

	msgsChan := activeSubscription.sub.MessagesChannel()
	for {
		select {
		case msg, ok := <-msgsChan:
			if !ok {
				return
			}
			brokerMsg := &BrokerMessage{
				Headers: msg.Headers,
				Payload: msg.Payload,
			}
			if err := handler(ctx, brokerMsg, FromBrokerMessage); err != nil {
				msg.Nak()
			} else {
				msg.Ack()
			}
		case <-ctx.Done():
			return
		}
	}
}
