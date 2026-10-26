package kafka

// The contents of this file represents additional structures and methods,
// suitable for mocking interfaces.

import (
	"context"
	"errors"
	"time"
)

// ControllerAdapter implements Controller with Sarama-backed producer and
// consumer adapters.
type ControllerAdapter struct {
	writer               *ControllerWriter
	consumerGroupFactory consumerGroupFactory
	nackBackoff          time.Duration
}

// NewControllerAdapter creates a new adapter for Kafka controller.
func NewControllerAdapter(
	writer *ControllerWriter,
	consumerGroupFactory consumerGroupFactory,
	nackBackoff time.Duration,
) *ControllerAdapter {
	return &ControllerAdapter{
		writer:               writer,
		consumerGroupFactory: consumerGroupFactory,
		nackBackoff:          nackBackoff,
	}
}

// Publish writes the messages through the asynchronous producer adapter.
func (ca *ControllerAdapter) Publish(
	ctx context.Context,
	topic string,
	msgs []BrokerMessage,
) ([]error, error) {
	return ca.writer.Publish(ctx, topic, msgs)
}

func (ca *ControllerAdapter) publishAsync(
	ctx context.Context,
	topic string,
	messages []BrokerMessage,
	complete func([]error, error),
) {
	ca.writer.publishAsync(ctx, topic, messages, complete)
}

// Subscribe starts a Sarama consumer-group subscription.
func (ca *ControllerAdapter) Subscribe(ctx context.Context, topic string) (Subscription, error) {
	if ca.consumerGroupFactory == nil {
		return nil, errors.New("consumer group factory is nil")
	}

	group, err := ca.consumerGroupFactory()
	if err != nil {
		return nil, err
	}
	if group == nil {
		return nil, errors.New("consumer group factory returned nil")
	}

	return newSubscriptionAdapter(ctx, topic, group, ca.nackBackoff), nil
}

// Close closes the producer adapter.
func (ca *ControllerAdapter) Close() error {
	return ca.writer.Close()
}
