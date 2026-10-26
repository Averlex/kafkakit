package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type consumeMessage func(context.Context, *sarama.ConsumerMessage, func()) bool

type consumerGroup struct {
	consumeFn func(context.Context, []string, consumeMessage) error
	closeFn   func() error
}

type consumerGroupFactory func() (*consumerGroup, error)

type saramaConsumerGroupHandler struct {
	consumeMessage consumeMessage
	autoCommit     bool
}

type consumerAcknowledgment struct {
	result chan bool
}

// SubscriptionAdapter adapts a Sarama consumer group to Subscription.
type SubscriptionAdapter struct {
	group        *consumerGroup
	messages     chan AcknowledgeableBrokerMessage
	done         chan struct{}
	cancel       context.CancelFunc
	topic        string
	nackBackoff  time.Duration
	cancelOnce   sync.Once
	shutdownOnce sync.Once
}

func newSaramaConsumerGroup(group sarama.ConsumerGroup, autoCommit bool) *consumerGroup {
	return &consumerGroup{
		consumeFn: func(ctx context.Context, topics []string, consume consumeMessage) error {
			return group.Consume(ctx, topics, &saramaConsumerGroupHandler{
				consumeMessage: consume,
				autoCommit:     autoCommit,
			})
		},
		closeFn: group.Close,
	}
}

func newSubscriptionAdapter(
	ctx context.Context,
	topic string,
	group *consumerGroup,
	nackBackoff time.Duration,
) *SubscriptionAdapter {
	subscriptionCtx, cancel := context.WithCancel(ctx)
	subscription := &SubscriptionAdapter{
		group:       group,
		messages:    make(chan AcknowledgeableBrokerMessage),
		done:        make(chan struct{}),
		cancel:      cancel,
		topic:       topic,
		nackBackoff: nackBackoff,
	}

	go subscription.run(subscriptionCtx)

	return subscription
}

// MessagesChannel returns messages delivered by the consumer group.
func (s *SubscriptionAdapter) MessagesChannel() <-chan AcknowledgeableBrokerMessage {
	return s.messages
}

// Cancel stops the consumer group and waits for bounded cleanup.
func (s *SubscriptionAdapter) Cancel(ctx context.Context) {
	s.cancelOnce.Do(s.cancel)

	select {
	case <-s.done:
	case <-ctx.Done():
	}
}

func (s *SubscriptionAdapter) run(ctx context.Context) {
	defer close(s.done)
	defer close(s.messages)
	defer s.shutdown()

	for ctx.Err() == nil {
		if err := s.group.consumeFn(ctx, []string{s.topic}, s.consume); err != nil {
			return
		}
	}
}

func (s *SubscriptionAdapter) consume(
	ctx context.Context,
	message *sarama.ConsumerMessage,
	acknowledge func(),
) bool {
	for {
		acknowledgment := &consumerAcknowledgment{result: make(chan bool, 1)}
		brokerMessage := NewAcknowledgeableBrokerMessage(
			BrokerMessage{
				Headers: consumerHeaders(message.Headers),
				Payload: message.Value,
			},
			acknowledgment,
		)

		select {
		case s.messages <- brokerMessage:
		case <-ctx.Done():
			return false
		}

		select {
		case accepted := <-acknowledgment.result:
			if accepted {
				acknowledge()

				return true
			}
		case <-ctx.Done():
			return false
		}

		if !waitForConsumerRetry(ctx, s.nackBackoff) {
			return false
		}
	}
}

func (s *SubscriptionAdapter) shutdown() {
	s.shutdownOnce.Do(func() {
		_ = s.group.closeFn()
	})
}

// Setup implements sarama.ConsumerGroupHandler.
func (h *saramaConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup implements sarama.ConsumerGroupHandler.
func (h *saramaConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim forwards messages one at a time and commits only acknowledged
// deliveries.
func (h *saramaConsumerGroupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if message == nil {
				continue
			}

			acknowledge := func() {
				session.MarkMessage(message, "")
				if !h.autoCommit {
					session.Commit()
				}
			}
			if !h.consumeMessage(session.Context(), message, acknowledge) {
				return nil
			}

		case <-session.Context().Done():
			return nil
		}
	}
}

// AckMessage accepts the current delivery.
func (a *consumerAcknowledgment) AckMessage() {
	a.result <- true
}

// NakMessage rejects the current delivery so it can be retried.
func (a *consumerAcknowledgment) NakMessage() {
	a.result <- false
}

func consumerHeaders(headers []*sarama.RecordHeader) map[string][]byte {
	if len(headers) == 0 {
		return nil
	}

	result := make(map[string][]byte, len(headers))
	for _, header := range headers {
		if header == nil {
			continue
		}
		result[string(header.Key)] = header.Value
	}

	return result
}

func waitForConsumerRetry(ctx context.Context, backoff time.Duration) bool {
	timer := time.NewTimer(backoff)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
