package kafka

import (
	"context"
)

// Controller defines the interface for Kafka controller operations.
type Controller interface {
	Publish(ctx context.Context, topic string, msgs []BrokerMessage) ([]error, error)
	Subscribe(ctx context.Context, topic string) (Subscription, error)
	Close() error
}

// Subscription defines the interface for Kafka subscription operations.
type Subscription interface {
	MessagesChannel() <-chan AcknowledgeableBrokerMessage
	Cancel(ctx context.Context)
}

// ControllerFactory creates Kafka controllers.
type ControllerFactory interface {
	New(cfg Config) (Controller, error)
}
