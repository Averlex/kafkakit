package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/Averlex/kafkakit"
	"github.com/Averlex/kafkakit/mocks"
)

type SubscribeHeaders struct {
	CorrelationID string `json:"correlation_id"`
	TraceID       string `json:"trace_id,omitempty"`
}

type SubscribePayload struct {
	UserID string `json:"user_id"`
	Action string `json:"action"`
}

type SubscribeMessage struct {
	Headers SubscribeHeaders
	Payload SubscribePayload
}

type SubscribeNestedHeaders struct {
	Meta struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	} `json:"meta"`
}

type SubscribeNestedPayload struct {
	Items []struct {
		Name  string            `json:"name"`
		Tags  []string          `json:"tags"`
		Attrs map[string]string `json:"attrs"`
	} `json:"items"`
}

type SubscribeNestedMessage struct {
	Headers SubscribeNestedHeaders
	Payload SubscribeNestedPayload
}

type SubscribeSuite struct {
	suite.Suite
	mockCtrl    *mocks.MockController
	mockFactory *mocks.MockControllerFactory
	mockSub     *mocks.MockSubscription
}

type UnsubscribeSuite struct {
	suite.Suite
	mockCtrl    *mocks.MockController
	mockFactory *mocks.MockControllerFactory
	mockSub     *mocks.MockSubscription
}

type acknowledgmentProbe struct {
	acked  chan struct{}
	nacked chan struct{}
}

func newAcknowledgmentProbe() *acknowledgmentProbe {
	return &acknowledgmentProbe{
		acked:  make(chan struct{}, 1),
		nacked: make(chan struct{}, 1),
	}
}

func (p *acknowledgmentProbe) AckMessage() {
	p.acked <- struct{}{}
}

func (p *acknowledgmentProbe) NakMessage() {
	p.nacked <- struct{}{}
}

func TestSubscribeSuite(t *testing.T) {
	suite.Run(t, new(SubscribeSuite))
}

func TestUnsubscribeSuite(t *testing.T) {
	suite.Run(t, new(UnsubscribeSuite))
}

func (s *SubscribeSuite) SetupTest() {
	s.mockCtrl = mocks.NewMockController(s.T())
	s.Require().NotNil(s.mockCtrl, "expected not nil controller but got nil")
	s.mockFactory = mocks.NewMockControllerFactory(s.T())
	s.Require().NotNil(s.mockFactory, "expected not nil factory but got nil")
	s.mockSub = mocks.NewMockSubscription(s.T())
	s.Require().NotNil(s.mockSub, "expected not nil subscription but got nil")
}

func (s *SubscribeSuite) TearDownTest() {
	s.Require().True(s.mockCtrl.AssertExpectations(s.T()), "controller expectations failed")
	s.Require().True(s.mockFactory.AssertExpectations(s.T()), "factory expectations failed")
	s.Require().True(s.mockSub.AssertExpectations(s.T()), "subscription expectations failed")
	s.mockCtrl = nil
	s.mockFactory = nil
	s.mockSub = nil
}

func (s *UnsubscribeSuite) SetupTest() {
	s.mockCtrl = mocks.NewMockController(s.T())
	s.Require().NotNil(s.mockCtrl, "expected not nil controller but got nil")
	s.mockFactory = mocks.NewMockControllerFactory(s.T())
	s.Require().NotNil(s.mockFactory, "expected not nil factory but got nil")
	s.mockSub = mocks.NewMockSubscription(s.T())
	s.Require().NotNil(s.mockSub, "expected not nil subscription but got nil")
}

func (s *UnsubscribeSuite) TearDownTest() {
	s.Require().True(s.mockCtrl.AssertExpectations(s.T()), "controller expectations failed")
	s.Require().True(s.mockFactory.AssertExpectations(s.T()), "factory expectations failed")
	s.Require().True(s.mockSub.AssertExpectations(s.T()), "subscription expectations failed")
	s.mockCtrl = nil
	s.mockFactory = nil
	s.mockSub = nil
}

func marshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic("marshal failed in test: " + err.Error())
	}
	return b
}

func makeAckMessage(headers map[string][]byte, payload any) kafka.AcknowledgeableBrokerMessage {
	return kafka.AcknowledgeableBrokerMessage{
		BrokerMessage: kafka.BrokerMessage{
			Headers: headers,
			Payload: marshal(payload),
		},
	}
}

func makeControlledMessage(payload any, probe *acknowledgmentProbe) kafka.AcknowledgeableBrokerMessage {
	return kafka.NewAcknowledgeableBrokerMessage(
		kafka.BrokerMessage{Payload: marshal(payload)},
		probe,
	)
}

func successfulHandler(
	handler func(context.Context, any, func(any, any) error),
) func(context.Context, any, func(any, any) error) error {
	return func(ctx context.Context, msg any, unmarshal func(any, any) error) error {
		handler(ctx, msg, unmarshal)

		return nil
	}
}

func (s *SubscribeSuite) TestSubscribe_Valid() {
	testCases := []struct {
		name          string
		messageCount  int
		useUnmarshal  bool
		headers       map[string][]byte
		payload       any
		cancelContext bool
	}{
		{
			name:         "simple",
			messageCount: 1,
			useUnmarshal: false,
			headers:      nil,
			payload:      struct{ UserID, Action string }{"user-id-simple", "create"},
		},
		{
			name:         "multiple messages",
			messageCount: 3,
			useUnmarshal: false,
			headers:      nil,
			payload:      struct{ UserID, Action string }{"user-id-multi", "update"},
		},
		{
			name:          "context cancellation",
			messageCount:  1,
			useUnmarshal:  false,
			headers:       nil,
			payload:       struct{ UserID, Action string }{"user-id-cancel", "delete"},
			cancelContext: true,
		},
		{
			name:         "consecutive subscribe",
			messageCount: 1,
			useUnmarshal: false,
			headers:      nil,
			payload:      struct{ UserID, Action string }{"user-id-consec", "read"},
		},
		{
			name:         "handler with unmarshal",
			messageCount: 1,
			useUnmarshal: true,
			headers:      map[string][]byte{"correlation_id": marshal("correlation-id-unmarshal")},
			payload:      struct{ UserID, Action string }{"user-id-unmarshal", "process"},
		},
		{
			name:         "zero value message",
			messageCount: 1,
			useUnmarshal: true,
			headers:      map[string][]byte{"correlation_id": marshal(""), "trace_id": marshal("")},
			payload:      struct{ UserID, Action string }{"", ""},
		},
		{
			name:         "nested and complex types",
			messageCount: 1,
			useUnmarshal: true,
			headers:      map[string][]byte{"correlation_id": marshal("correlation-id-nested")},
			payload: struct {
				Items []struct {
					Name  string            `json:"name"`
					Tags  []string          `json:"tags"`
					Attrs map[string]string `json:"attrs"`
				}
			}{
				Items: []struct {
					Name  string            `json:"name"`
					Tags  []string          `json:"tags"`
					Attrs map[string]string `json:"attrs"`
				}{
					{Name: "item1", Tags: []string{"a", "b"}, Attrs: map[string]string{"color": "red"}},
				},
			},
		},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			var ctx context.Context
			var cancel context.CancelFunc
			if tC.cancelContext {
				ctx, cancel = context.WithCancel(context.Background())
				defer cancel()
			} else {
				ctx = context.Background()
			}

			s.mockFactory.On("New", mock.Anything).
				Return(s.mockCtrl, nil).Once()

			msgsChan := make(chan kafka.AcknowledgeableBrokerMessage, tC.messageCount)
			for range tC.messageCount {
				msgsChan <- makeAckMessage(tC.headers, tC.payload)
			}
			close(msgsChan)

			s.mockCtrl.On("Subscribe", mock.Anything, mock.Anything).
				Return(s.mockSub, nil).Once()
			s.mockSub.On("MessagesChannel").Return((<-chan kafka.AcknowledgeableBrokerMessage)(msgsChan)).Once()
			if !tC.cancelContext {
				s.mockSub.On("Cancel", mock.Anything).Return().Maybe()
			}

			cfg := kafka.Config{
				Mode:              kafka.ModeMixed,
				DSN:               testDSN,
				GroupID:           testGroupID,
				AutoCommit:        true,
				ControllerFactory: s.mockFactory,
				MaxBytes:          testMaxBytes,
			}
			broker, err := kafka.NewBroker(&cfg)
			s.Require().NoError(err, "new broker")
			s.Require().NotNil(broker, "broker is nil")

			err = broker.Connect(ctx)
			s.Require().NoError(err, "connect failed")

			var handlerCallCount int
			var mu sync.Mutex

			handler := func(_ context.Context, msg any, unmarshal func(any, any) error) {
				mu.Lock()
				handlerCallCount++
				mu.Unlock()

				if tC.useUnmarshal {
					if tC.name == "nested and complex types" {
						var m SubscribeNestedMessage
						s.Require().NoError(unmarshal(msg, &m), "unmarshal failed")
					} else {
						var m SubscribeMessage
						s.Require().NoError(unmarshal(msg, &m), "unmarshal failed")
					}
				}
			}

			done, err := broker.Subscribe(ctx, "test-topic", successfulHandler(handler))
			s.Require().NoError(err, "subscribe failed")
			s.Require().NotNil(done, "done channel is nil")

			if tC.cancelContext {
				cancel()
			}

			<-done

			mu.Lock()
			actualCalls := handlerCallCount
			mu.Unlock()

			if !tC.cancelContext {
				s.Require().Equal(tC.messageCount, actualCalls, "handler call count mismatch")
			}

			if tC.name == "consecutive subscribe" {
				msgsChan2 := make(chan kafka.AcknowledgeableBrokerMessage, 1)
				msgsChan2 <- makeAckMessage(nil, struct{ UserID, Action string }{"user-id-consec-2", "read2"})
				close(msgsChan2)

				s.mockCtrl.On("Subscribe", mock.Anything, "test-topic-2").
					Return(s.mockSub, nil).Once()
				s.mockSub.On("MessagesChannel").Return((<-chan kafka.AcknowledgeableBrokerMessage)(msgsChan2)).Once()

				done2, err := broker.Subscribe(ctx, "test-topic-2", successfulHandler(handler))
				s.Require().NoError(err, "second subscribe failed")
				s.Require().NotNil(done2, "second done channel is nil")
				<-done2
			}
		})
	}
}

func (s *SubscribeSuite) TestSubscribe_Fail() {
	testCases := []struct {
		name          string
		handler       func(context.Context, any, func(any, any) error)
		setupMocks    bool
		subscribeErr  error
		expectConnect bool
		alreadySub    bool
	}{
		{
			name:          "not connected",
			handler:       func(_ context.Context, _ any, _ func(any, any) error) {},
			expectConnect: false,
		},
		{
			name:          "nil handler",
			handler:       nil,
			expectConnect: true,
		},
		{
			name:          "already subscribed",
			handler:       func(_ context.Context, _ any, _ func(any, any) error) {},
			expectConnect: true,
			alreadySub:    true,
		},
		{
			name:          "subscribe error",
			handler:       func(_ context.Context, _ any, _ func(any, any) error) {},
			setupMocks:    true,
			subscribeErr:  errors.New("controller subscribe failed"),
			expectConnect: true,
		},
		{
			name:          "controller nil",
			handler:       func(_ context.Context, _ any, _ func(any, any) error) {},
			expectConnect: false,
		},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			ctx := context.Background()
			messagesChannelRequested := make(chan struct{}, 1)

			if tC.setupMocks || tC.expectConnect || tC.alreadySub {
				s.mockFactory.On("New", mock.Anything).
					Return(s.mockCtrl, nil).Maybe()
			}

			if tC.subscribeErr != nil {
				s.mockCtrl.On("Subscribe", mock.Anything, mock.Anything).
					Return(nil, tC.subscribeErr).Once()
			}

			cfg := kafka.Config{
				Mode:              kafka.ModeMixed,
				DSN:               testDSN,
				GroupID:           testGroupID,
				AutoCommit:        true,
				ControllerFactory: s.mockFactory,
				MaxBytes:          testMaxBytes,
			}
			broker, err := kafka.NewBroker(&cfg)
			s.Require().NoError(err, "new broker")
			s.Require().NotNil(broker, "broker is nil")

			if tC.expectConnect {
				err = broker.Connect(ctx)
				s.Require().NoError(err, "connect failed")
			}

			if tC.alreadySub {
				mockMsgsChan := make(chan kafka.AcknowledgeableBrokerMessage)
				s.mockCtrl.On("Subscribe", mock.Anything, "test-topic").
					Return(s.mockSub, nil).Once()
				s.mockSub.On("MessagesChannel").
					Run(func(mock.Arguments) { messagesChannelRequested <- struct{}{} }).
					Return((<-chan kafka.AcknowledgeableBrokerMessage)(mockMsgsChan)).Once()
				s.mockSub.On("Cancel", mock.Anything).Return().Once()

				_, err := broker.Subscribe(
					ctx,
					"test-topic",
					successfulHandler(func(_ context.Context, _ any, _ func(any, any) error) {}),
				)
				s.Require().NoError(err, "first subscribe failed")
				select {
				case <-messagesChannelRequested:
				case <-time.After(time.Second):
					s.Require().FailNow("message processor must start", "timed out waiting for message channel")
				}
			}

			var handler func(context.Context, any, func(any, any) error) error
			if tC.handler != nil {
				handler = successfulHandler(tC.handler)
			}
			_, err = broker.Subscribe(ctx, "test-topic", handler)
			s.Require().Error(err, "expected error but got nil")

			if tC.alreadySub {
				err = broker.Unsubscribe(ctx, "test-topic")
				s.Require().NoError(err, "cleanup unsubscribe failed")
			}
		})
	}
}

func (s *SubscribeSuite) TestSubscribe_Concurrent() {
	ctx := context.Background()

	s.mockFactory.On("New", mock.Anything).
		Return(s.mockCtrl, nil).Once()

	const concurrentCount = 5

	mockSubs := make([]*mocks.MockSubscription, concurrentCount)
	for i := range concurrentCount {
		mockSubs[i] = mocks.NewMockSubscription(s.T())
		msgChan := make(chan kafka.AcknowledgeableBrokerMessage, 1)
		msgChan <- makeAckMessage(
			map[string][]byte{"correlation_id": marshal("correlation-id-concurrent-" + string(rune('0'+i)))},
			struct{ UserID, Action string }{"user-id-concurrent-" + string(rune('0'+i)), "concurrent"},
		)
		close(msgChan)
		mockSubs[i].On("MessagesChannel").
			Return((<-chan kafka.AcknowledgeableBrokerMessage)(msgChan)).Once()
	}

	for i := range concurrentCount {
		s.mockCtrl.On("Subscribe", mock.Anything, mock.Anything).
			Return(mockSubs[i], nil).Once()
	}

	cfg := kafka.Config{
		Mode:              kafka.ModeMixed,
		DSN:               testDSN,
		GroupID:           testGroupID,
		AutoCommit:        true,
		ControllerFactory: s.mockFactory,
		MaxBytes:          testMaxBytes,
	}
	broker, err := kafka.NewBroker(&cfg)
	s.Require().NoError(err, "new broker")
	s.Require().NotNil(broker, "broker is nil")

	err = broker.Connect(ctx)
	s.Require().NoError(err, "connect failed")

	var wg sync.WaitGroup
	var mu sync.Mutex
	handlerCallCount := 0

	for i := range concurrentCount {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			topic := "topic-concurrent-" + string(rune('0'+id))
			handler := func(_ context.Context, msg any, unmarshal func(any, any) error) {
				var m SubscribeMessage
				err := unmarshal(msg, &m)
				s.Require().NoError(err, "concurrent unmarshal failed")
				mu.Lock()
				handlerCallCount++
				mu.Unlock()
			}

			done, err := broker.Subscribe(ctx, topic, successfulHandler(handler))
			s.Require().NoError(err, "concurrent subscribe failed")
			s.Require().NotNil(done, "done channel is nil")
			<-done
		}(i)
	}

	wg.Wait()
	s.Require().Equal(concurrentCount, handlerCallCount, "concurrent handler call count mismatch")

	for _, sub := range mockSubs {
		s.Require().True(sub.AssertExpectations(s.T()), "subscription expectations failed")
	}
}

func (s *SubscribeSuite) TestSubscribe_HandlerResultControlsAcknowledgment() {
	testCases := []struct {
		handlerErr error
		name       string
		expectAck  bool
	}{
		{
			name:      "successful handler acknowledges",
			expectAck: true,
		},
		{
			name:       "failed handler negatively acknowledges",
			handlerErr: errors.New("retry message"),
		},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			s.mockFactory.On("New", mock.Anything).Return(s.mockCtrl, nil).Once()

			probe := newAcknowledgmentProbe()
			messages := make(chan kafka.AcknowledgeableBrokerMessage, 1)
			messages <- makeControlledMessage(struct{}{}, probe)
			close(messages)

			s.mockCtrl.On("Subscribe", mock.Anything, "test-topic").Return(s.mockSub, nil).Once()
			s.mockSub.On("MessagesChannel").
				Return((<-chan kafka.AcknowledgeableBrokerMessage)(messages)).Once()

			broker, err := kafka.NewBroker(&kafka.Config{
				Mode:              kafka.ModeConsumer,
				DSN:               testDSN,
				GroupID:           testGroupID,
				MaxBytes:          testMaxBytes,
				ControllerFactory: s.mockFactory,
			})
			s.Require().NoError(err, "new broker")
			s.Require().NoError(broker.Connect(s.T().Context()), "connect broker")

			done, err := broker.Subscribe(
				s.T().Context(),
				"test-topic",
				func(context.Context, any, func(any, any) error) error {
					return tC.handlerErr
				},
			)
			s.Require().NoError(err, "subscribe")
			<-done

			if tC.expectAck {
				s.Require().NotEmpty(probe.acked, "successful handler must acknowledge the message")
				s.Require().Empty(probe.nacked, "successful handler must not negatively acknowledge the message")
			} else {
				s.Require().Empty(probe.acked, "failed handler must not acknowledge the message")
				s.Require().NotEmpty(probe.nacked, "failed handler must negatively acknowledge the message")
			}
		})
	}
}

func (s *UnsubscribeSuite) TestUnsubscribe_Valid() {
	testCases := []struct {
		name      string
		topics    []string
		useGlobal bool
	}{
		{
			name:      "simple",
			topics:    []string{"topic-1"},
			useGlobal: true,
		},
		{
			name:      "multiple topics",
			topics:    []string{"topic-a", "topic-b", "topic-c"},
			useGlobal: false,
		},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			ctx := context.Background()
			var done <-chan struct{}

			s.mockFactory.On("New", mock.Anything).
				Return(s.mockCtrl, nil).Once()

			var mockSubs []*mocks.MockSubscription
			doneByTopic := make(map[string]<-chan struct{}, len(tC.topics))

			if tC.useGlobal {
				messages := make(chan kafka.AcknowledgeableBrokerMessage)
				s.mockSub.On("MessagesChannel").
					Return((<-chan kafka.AcknowledgeableBrokerMessage)(messages)).Once()
				s.mockSub.On("Cancel", mock.Anything).Return().Once()
				s.mockCtrl.On("Subscribe", mock.Anything, mock.Anything).
					Return(s.mockSub, nil).Once()
				mockSubs = []*mocks.MockSubscription{s.mockSub}
			} else {
				mockSubs = make([]*mocks.MockSubscription, len(tC.topics))
				for i := range tC.topics {
					mockSub := mocks.NewMockSubscription(s.T())
					messages := make(chan kafka.AcknowledgeableBrokerMessage)
					mockSub.On("MessagesChannel").
						Return((<-chan kafka.AcknowledgeableBrokerMessage)(messages)).Once()
					mockSub.On("Cancel", mock.Anything).Return().Once()
					s.mockCtrl.On("Subscribe", mock.Anything, mock.Anything).
						Return(mockSub, nil).Once()
					mockSubs[i] = mockSub
				}
			}

			cfg := kafka.Config{
				Mode:              kafka.ModeMixed,
				DSN:               testDSN,
				GroupID:           testGroupID,
				AutoCommit:        true,
				ControllerFactory: s.mockFactory,
				MaxBytes:          testMaxBytes,
			}
			broker, err := kafka.NewBroker(&cfg)
			s.Require().NoError(err, "new broker")
			s.Require().NotNil(broker, "broker is nil")

			err = broker.Connect(ctx)
			s.Require().NoError(err, "connect failed")

			for _, topic := range tC.topics {
				done, err = broker.Subscribe(
					ctx,
					topic,
					successfulHandler(func(_ context.Context, _ any, _ func(any, any) error) {}),
				)
				s.Require().NoError(err, "subscribe failed")
				s.Require().NotNil(done, "done channel is nil")
				doneByTopic[topic] = done
			}

			for _, topic := range tC.topics {
				err := broker.Unsubscribe(ctx, topic)
				s.Require().NoError(err, "unsubscribe failed")
				<-doneByTopic[topic]
			}

			if !tC.useGlobal {
				for _, sub := range mockSubs {
					s.Require().True(sub.AssertExpectations(s.T()), "subscription expectations failed")
				}
			}
		})
	}
}

func (s *UnsubscribeSuite) TestUnsubscribe_Fail() {
	testCases := []struct {
		name   string
		topic  string
		hasSub bool
	}{
		{"no subscription", "unknown", false},
		{"consecutive calls", "topic-1", true},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			ctx := context.Background()
			var done <-chan struct{}

			cfg := kafka.Config{
				Mode:       kafka.ModeMixed,
				DSN:        testDSN,
				GroupID:    testGroupID,
				AutoCommit: true,
				MaxBytes:   testMaxBytes,
			}

			if tC.hasSub {
				s.mockFactory.On("New", mock.Anything).
					Return(s.mockCtrl, nil).Once()
				s.mockCtrl.On("Subscribe", mock.Anything, mock.Anything).
					Return(s.mockSub, nil).Once()

				messages := make(chan kafka.AcknowledgeableBrokerMessage)
				s.mockSub.On("MessagesChannel").
					Return((<-chan kafka.AcknowledgeableBrokerMessage)(messages)).Once()
				s.mockSub.On("Cancel", mock.Anything).Return().Once()

				cfg.ControllerFactory = s.mockFactory
			}

			broker, err := kafka.NewBroker(&cfg)
			s.Require().NoError(err, "new broker")
			s.Require().NotNil(broker, "broker is nil")

			if tC.hasSub {
				err = broker.Connect(ctx)
				s.Require().NoError(err, "connect failed")

				done, err = broker.Subscribe(
					ctx,
					tC.topic,
					successfulHandler(func(_ context.Context, _ any, _ func(any, any) error) {}),
				)
				s.Require().NoError(err, "subscribe failed")
				s.Require().NotNil(done, "done channel is nil")
			}

			err = broker.Unsubscribe(ctx, tC.topic)
			if tC.hasSub {
				s.Require().NoError(err, "first unsubscribe failed")
				<-done
				err = broker.Unsubscribe(ctx, tC.topic)
			}
			s.Require().Error(err, "expected error")
		})
	}
}

func (s *UnsubscribeSuite) TestUnsubscribe_Concurrent() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.mockFactory.On("New", mock.Anything).
		Return(s.mockCtrl, nil).Once()

	const concurrentCount = 5

	// Individual mocks for each subscription.
	mockSubs := make([]*mocks.MockSubscription, concurrentCount)
	for i := range mockSubs {
		mockSubs[i] = mocks.NewMockSubscription(s.T())
		messages := make(chan kafka.AcknowledgeableBrokerMessage)
		mockSubs[i].On("MessagesChannel").
			Return((<-chan kafka.AcknowledgeableBrokerMessage)(messages)).Once()
		mockSubs[i].On("Cancel", mock.Anything).Return().Once()
	}

	for i := range mockSubs {
		s.mockCtrl.On("Subscribe", mock.Anything, mock.Anything).
			Return(mockSubs[i], nil).Once()
	}

	cfg := kafka.Config{
		Mode:              kafka.ModeMixed,
		DSN:               testDSN,
		GroupID:           testGroupID,
		AutoCommit:        true,
		ControllerFactory: s.mockFactory,
		MaxBytes:          testMaxBytes,
	}
	broker, err := kafka.NewBroker(&cfg)
	s.Require().NoError(err, "new broker")
	s.Require().NotNil(broker, "broker is nil")

	err = broker.Connect(ctx)
	s.Require().NoError(err, "connect failed")

	// Subscribe to all topics first.
	topics := make([]string, concurrentCount)
	doneChannels := make([]<-chan struct{}, concurrentCount)
	for i := range topics {
		topics[i] = "topic-concurrent-" + string(rune('0'+i))
		done, err := broker.Subscribe(
			ctx,
			topics[i],
			successfulHandler(func(_ context.Context, _ any, _ func(any, any) error) {}),
		)
		s.Require().NoError(err, "subscribe failed")
		s.Require().NotNil(done, "done channel is nil")

		doneChannels[i] = done
	}

	// Concurrent unsubscribe.
	unsubscribeErrors := make(chan error, concurrentCount)

	for _, topic := range topics {
		go func(t string) {
			unsubscribeErrors <- broker.Unsubscribe(ctx, t)
		}(topic)
	}

	for range topics {
		select {
		case err := <-unsubscribeErrors:
			s.Require().NoError(err, "concurrent unsubscribe failed")
		case <-ctx.Done():
			s.Require().FailNow("timed out waiting for concurrent unsubscribe")
		}
	}

	// Cancellation returns before processing stops; wait before checking mocks.
	for _, done := range doneChannels {
		select {
		case <-done:
		case <-ctx.Done():
			s.Require().FailNow("timed out waiting for subscription processing to stop")
		}
	}

	// Verify all expectations.
	for _, sub := range mockSubs {
		s.Require().True(sub.AssertExpectations(s.T()), "subscription expectations failed")
	}
}
