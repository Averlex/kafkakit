// Package kafka provides synchronous and asynchronous Kafka publishing and
// subscription methods.
//
// Messages may be any JSON-serializable value. Byte slices are transported as
// raw payloads. Structs with exported Headers and Payload fields retain the
// envelope format: both parts use JSON, and JSON-tagged fields from the Headers
// struct are transported as Kafka headers. Other message forms are transported
// without Kafka headers.
//
// The client supports producer-only, consumer-only, and mixed modes.
// All core components are exposed as interfaces for testing.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
)

// Broker is a client for interacting with Kafka.
type Broker struct {
	controller Controller
	subs       map[string]*subscription
	cfg        Config
	mu         sync.Mutex
}

// NewBroker creates a new Kafka client.
func NewBroker(config *Config) (*Broker, error) {
	if config == nil {
		return nil, errors.New("no config passed")
	}

	cfg := DefaultConfig()
	cfg.DSN = config.DSN
	cfg.Hosts = config.Hosts
	cfg.GroupID = config.GroupID
	cfg.MaxBytes = config.MaxBytes
	cfg.AutoCommit = config.AutoCommit
	cfg.ConnectionTest = config.ConnectionTest
	cfg.Mode = config.Mode
	cfg.TopicCreateAttempts = config.TopicCreateAttempts

	// Zero values keep the defaults, so that callers only have to set these
	// when they want to override the related behaviour.
	if config.TopicCreateBackoff > 0 {
		cfg.TopicCreateBackoff = config.TopicCreateBackoff
	}
	if config.BatchSize > 0 {
		cfg.BatchSize = config.BatchSize
	}
	if config.BatchBytes != 0 {
		cfg.BatchBytes = config.BatchBytes
	}
	if config.BatchTimeout > 0 {
		cfg.BatchTimeout = config.BatchTimeout
	}
	if config.NackBackoff != 0 {
		cfg.NackBackoff = config.NackBackoff
	}
	if config.ControllerFactory != nil {
		cfg.ControllerFactory = config.ControllerFactory
	}
	if config.SaramaFactory != nil {
		cfg.SaramaFactory = config.SaramaFactory
	}

	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("ivalid config: %w", err)
	}

	return &Broker{
		cfg:  cfg,
		subs: make(map[string]*subscription),
	}, nil
}

// Connect establishes connection with Kafka (idempotently).
func (b *Broker) Connect(_ context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.controller != nil {
		return nil
	}

	ctrl, err := b.cfg.ControllerFactory.New(b.cfg)
	if err != nil {
		return fmt.Errorf("create kafka controller: %w", err)
	}

	b.controller = ctrl
	return nil
}

// Close drops all active subscriptions (idempotently).
func (b *Broker) Close(ctx context.Context) {
	b.mu.Lock()
	if b.controller == nil {
		b.mu.Unlock()
		return
	}

	controller := b.controller
	b.controller = nil

	subscriptions := make([]*subscription, 0, len(b.subs))
	for topic, activeSubscription := range b.subs {
		subscriptions = append(subscriptions, activeSubscription)
		delete(b.subs, topic)
	}
	b.mu.Unlock()

	for _, s := range subscriptions {
		s.cancel()
		s.sub.Cancel(ctx)
	}

	if err := controller.Close(); err != nil {
		log.Printf("close kafka controller: %v", err)
	}
}

// subscription holds a Kafka subscription and its cancellation function.
type subscription struct {
	sub    Subscription
	cancel context.CancelFunc
}
