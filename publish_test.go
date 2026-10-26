package kafka_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/Averlex/kafkakit"
	"github.com/Averlex/kafkakit/mocks"
)

// region Suite

type PublishSuite struct {
	suite.Suite
	mockCtrl    *mocks.MockController
	mockFactory *mocks.MockControllerFactory
}

func TestPublishSuite(t *testing.T) {
	suite.Run(t, new(PublishSuite))
}

func (s *PublishSuite) SetupTest() {
	s.mockCtrl = mocks.NewMockController(s.T())
	s.Require().NotNil(s.mockCtrl, "expected not nil controller but got nil")
	s.mockFactory = mocks.NewMockControllerFactory(s.T())
	s.Require().NotNil(s.mockFactory, "expected not nil factory but got nil")
}

func (s *PublishSuite) TearDownTest() {
	s.Require().True(s.mockCtrl.AssertExpectations(s.T()), "controller expectations failed")
	s.Require().True(s.mockFactory.AssertExpectations(s.T()), "factory expectations failed")
	s.mockCtrl = nil
	s.mockFactory = nil
}

// endregion

// region Structs

type PublishHeaders struct {
	CorrelationID string `json:"correlation_id"`
	TraceID       string `json:"trace_id,omitempty"`
}

type PublishPayload struct {
	UserID string `json:"user_id"`
	Action string `json:"action"`
}

type PublishMessage struct {
	Headers PublishHeaders
	Payload PublishPayload
}

type PublishNestedHeaders struct {
	Meta struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	} `json:"meta"`
}

type PublishNestedPayload struct {
	Items []struct {
		Name  string            `json:"name"`
		Tags  []string          `json:"tags"`
		Attrs map[string]string `json:"attrs"`
	} `json:"items"`
}

type PublishMixedHeaders struct {
	ID    string `json:"id"`
	Type  string
	Token string `json:"-"`
}

type PublishMixedPayload struct {
	Name  string            `json:"name"`
	Tags  []string          `json:""`
	Attrs map[string]string `json:"-"`
}

type PublishNestedMessage struct {
	Headers PublishNestedHeaders
	Payload PublishNestedPayload
}

type PublishMixedMessage struct {
	Headers PublishMixedHeaders
	Payload PublishMixedPayload
}

type PublishPointerHeaders struct {
	Headers *PublishHeaders
	Payload PublishPayload
}

type PublishPointerPayload struct {
	Headers PublishHeaders
	Payload *PublishPayload
}

type PublishZeroMessage struct {
	Headers PublishHeaders
	Payload PublishPayload
}

type PublishStringPayloadMessage struct {
	Headers PublishHeaders
	Payload string
}

// endregion

// region Tests

func (s *PublishSuite) TestPublish_Valid() {
	testCases := []struct {
		name    string
		message any
	}{
		{
			name: "simple message",
			message: PublishMessage{
				Headers: PublishHeaders{CorrelationID: "correlation-id-simple", TraceID: "trace-id-simple"},
				Payload: PublishPayload{UserID: "user-id-simple", Action: "create"},
			},
		},
		{
			name: "pointer fields",
			message: &PublishMessage{
				Headers: PublishHeaders{CorrelationID: "correlation-id-pointer"},
				Payload: PublishPayload{UserID: "user-id-pointer", Action: "update"},
			},
		},
		{
			name: "pointer headers, value payload",
			message: PublishPointerHeaders{
				Headers: &PublishHeaders{CorrelationID: "cid-3"},
				Payload: PublishPayload{UserID: "u3", Action: "delete"},
			},
		},
		{
			name: "value headers, pointer payload",
			message: PublishPointerPayload{
				Headers: PublishHeaders{CorrelationID: "cid-4"},
				Payload: &PublishPayload{UserID: "u4", Action: "read"},
			},
		},
		{
			name: "nested structs",
			message: PublishNestedMessage{
				Headers: PublishNestedHeaders{
					Meta: struct {
						ID   string `json:"id"`
						Type string `json:"type"`
					}{ID: "m1", Type: "event"},
				},
				Payload: PublishNestedPayload{
					Items: []struct {
						Name  string            `json:"name"`
						Tags  []string          `json:"tags"`
						Attrs map[string]string `json:"attrs"`
					}{
						{
							Name:  "item1",
							Tags:  []string{"a", "b"},
							Attrs: map[string]string{"color": "red"},
						},
					},
				},
			},
		},
		{
			name: "missing and skippable tags",
			message: PublishMixedMessage{
				Headers: PublishMixedHeaders{
					ID: "m1", Type: "event", Token: "something",
				},
				Payload: PublishMixedPayload{
					Name:  "item1",
					Tags:  []string{"a", "b"},
					Attrs: map[string]string{"color": "red"},
				},
			},
		},
		{
			name: "nil headers pointer",
			message: struct {
				Headers *PublishHeaders
				Payload PublishPayload
			}{
				Headers: nil,
				Payload: PublishPayload{UserID: "u", Action: "test"},
			},
		},
		{
			name: "slices and maps",
			message: PublishMessage{
				Headers: PublishHeaders{CorrelationID: "cid-5"},
				Payload: PublishPayload{UserID: "u5", Action: "batch"},
			},
		},
		{
			name: "zero values",
			message: PublishZeroMessage{
				Headers: PublishHeaders{},
				Payload: PublishPayload{},
			},
		},
		{
			name: "consecutive",
			message: PublishMessage{
				Headers: PublishHeaders{CorrelationID: "cid-6"},
				Payload: PublishPayload{UserID: "u6", Action: "consecutive"},
			},
		},
		{
			name:    "generic string",
			message: "plain string",
		},
		{
			name:    "generic slice as one message",
			message: []string{"first", "second"},
		},
		{
			name:    "raw bytes",
			message: []byte{0xff, 0x00, 0x7f},
		},
		{
			name: "generic struct",
			message: struct {
				Action string `json:"action"`
			}{Action: "generic"},
		},
		{
			name:    "typed nil pointer",
			message: (*PublishMessage)(nil),
		},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			ctx := context.Background()

			s.mockFactory.On("New", mock.Anything).
				Return(s.mockCtrl, nil).Once()

			callCount := 1
			if tC.name == "consecutive" {
				callCount = 2
			}
			for i := 0; i < callCount; i++ {
				s.mockCtrl.On("Publish", mock.Anything, mock.Anything, mock.Anything).
					Return([]error{nil}, nil).Once()
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

			err = broker.Publish(ctx, "test-topic", tC.message)
			s.Require().NoError(err, "publish failed")

			if tC.name == "consecutive" {
				err = broker.Publish(ctx, "test-topic", tC.message)
				s.Require().NoError(err, "second publish failed")
			}
		})
	}
}

func (s *PublishSuite) TestPublish_Fail() {
	testCases := []struct {
		name          string
		message       any
		setupMocks    bool
		publishErr    error
		expectConnect bool
	}{
		{
			name: "not connected",
			message: PublishMessage{
				Headers: PublishHeaders{},
				Payload: PublishPayload{},
			},
			expectConnect: false,
		},
		{
			name:          "nil message",
			message:       nil,
			expectConnect: true,
		},
		{
			name: "payload marshal",
			message: struct {
				Headers PublishHeaders
				Payload struct {
					Ch chan int `json:"ch"`
				}
			}{
				Headers: PublishHeaders{},
				Payload: struct {
					Ch chan int `json:"ch"`
				}{Ch: make(chan int)},
			},
			expectConnect: true,
		},
		{
			name:          "generic marshal",
			message:       make(chan int),
			expectConnect: true,
		},
		{
			name: "headers marshal",
			message: struct {
				Headers struct {
					F func() `json:"f"`
				}
				Payload PublishPayload
			}{
				Headers: struct {
					F func() `json:"f"`
				}{F: func() {}},
				Payload: PublishPayload{},
			},
			expectConnect: true,
		},
		{
			name:          "publish error",
			message:       PublishMessage{Headers: PublishHeaders{}, Payload: PublishPayload{}},
			setupMocks:    true,
			publishErr:    errors.New("controller publish failed"),
			expectConnect: true,
		},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			ctx := context.Background()

			if tC.setupMocks || tC.expectConnect {
				s.mockFactory.On("New", mock.Anything).
					Return(s.mockCtrl, nil).Once()
			}

			if tC.publishErr != nil {
				s.mockCtrl.On("Publish", mock.Anything, mock.Anything, mock.Anything).
					Return([]error{tC.publishErr}, nil).Once()
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

			err = broker.Publish(ctx, "test-topic", tC.message)
			s.Require().Error(err, "expected error but got nil")
		})
	}
}

func (s *PublishSuite) TestPublish_StringPayload() {
	ctx := context.Background()

	const rawPayload = "hello, world"

	var captured kafka.BrokerMessage
	s.mockFactory.On("New", mock.Anything).
		Return(s.mockCtrl, nil).Once()
	s.mockCtrl.On("Publish", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			captured = args.Get(2).([]kafka.BrokerMessage)[0]
		}).
		Return([]error{nil}, nil).Once()

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

	msg := PublishStringPayloadMessage{
		Headers: PublishHeaders{CorrelationID: "cid-str"},
		Payload: rawPayload,
	}
	err = broker.Publish(ctx, "test-topic", msg)
	s.Require().NoError(err, "publish failed")

	s.Require().Equal([]byte(`"`+rawPayload+`"`), captured.Payload, "string payload is json-encoded, symmetric with FromBrokerMessage")
}

func (s *PublishSuite) TestPublish_GenericEncoding() {
	testCases := []struct {
		message any
		name    string
		payload []byte
	}{
		{
			name:    "struct",
			message: GenericMessage{Action: "create", Items: []int{1, 2}},
			payload: []byte(`{"action":"create","items":[1,2],"meta":null}`),
		},
		{
			name:    "slice remains one message",
			message: []string{"first", "second"},
			payload: []byte(`["first","second"]`),
		},
		{
			name:    "primitive",
			message: 42,
			payload: []byte(`42`),
		},
		{
			name:    "raw bytes",
			message: []byte{0xff, 0x00, 0x7f},
			payload: []byte{0xff, 0x00, 0x7f},
		},
		{
			name:    "nil raw bytes",
			message: []byte(nil),
			payload: nil,
		},
		{
			name:    "typed nil",
			message: (*GenericMessage)(nil),
			payload: []byte(`null`),
		},
		{
			name:    "generic envelope field names",
			message: GenericMessageWithEnvelopeNames{Headers: "metadata", Payload: 42},
			payload: []byte(`{"headers":"metadata","payload":42}`),
		},
	}

	var captured []kafka.BrokerMessage
	s.mockFactory.On("New", mock.Anything).
		Return(s.mockCtrl, nil).Once()
	s.mockCtrl.On("Publish", mock.Anything, "test-topic", mock.Anything).
		Run(func(args mock.Arguments) {
			messages := args.Get(2).([]kafka.BrokerMessage)
			s.Require().Len(messages, 1, "Publish must send one broker message")
			captured = append(captured, messages[0])
		}).
		Return([]error{nil}, nil).Times(len(testCases))

	broker := s.newConnectedBroker(s.T().Context())
	for _, tC := range testCases {
		err := broker.Publish(s.T().Context(), "test-topic", tC.message)
		s.Require().NoError(err, "publish generic message %q", tC.name)
	}

	s.Require().Len(captured, len(testCases), "capture every published message")
	for i, tC := range testCases {
		s.Require().Equal(tC.payload, captured[i].Payload, "payload mismatch for %q", tC.name)
		s.Require().Empty(captured[i].Headers, "generic message %q must not add headers", tC.name)
	}
}

func (s *PublishSuite) TestPublish_Concurrent() {
	ctx := context.Background()

	s.mockFactory.On("New", mock.Anything).
		Return(s.mockCtrl, nil).Once()

	const concurrentCount = 10
	s.mockCtrl.On("Publish", mock.Anything, mock.Anything, mock.Anything).
		Return([]error{nil}, nil).Times(concurrentCount)

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
	for i := range concurrentCount {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			msg := PublishMessage{
				Headers: PublishHeaders{CorrelationID: "correlation-concurrent-" + string(rune('0'+id))},
				Payload: PublishPayload{UserID: "user-concurrent-" + string(rune('0'+id)), Action: "publish"},
			}
			err := broker.Publish(ctx, "test-topic", msg)
			s.Require().NoError(err, "concurrent publish failed")
		}(i)
	}

	wg.Wait()
}

func (s *PublishSuite) newConnectedBroker(ctx context.Context) *kafka.Broker {
	s.T().Helper()

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

	return broker
}

func (s *PublishSuite) TestPublishBatch_Valid() {
	ctx := context.Background()

	var captured []kafka.BrokerMessage
	s.mockFactory.On("New", mock.Anything).
		Return(s.mockCtrl, nil).Once()
	s.mockCtrl.On("Publish", mock.Anything, "test-topic", mock.Anything).
		Run(func(args mock.Arguments) {
			captured = args.Get(2).([]kafka.BrokerMessage)
		}).
		Return([]error{nil, nil}, nil).Once()

	broker := s.newConnectedBroker(ctx)

	msgs := []any{
		PublishMessage{
			Headers: PublishHeaders{CorrelationID: "cid-1"},
			Payload: PublishPayload{UserID: "user-1", Action: "create"},
		},
		&PublishMessage{
			Headers: PublishHeaders{CorrelationID: "cid-2"},
			Payload: PublishPayload{UserID: "user-2", Action: "update"},
		},
	}

	err := broker.PublishBatch(ctx, "test-topic", msgs)
	s.Require().NoError(err, "publish batch failed")
	s.Require().Len(captured, len(msgs), "whole batch goes to the controller in one call")
}

func (s *PublishSuite) TestPublishBatch_ResultsStayAlignedWithInput() {
	ctx := context.Background()

	s.mockFactory.On("New", mock.Anything).
		Return(s.mockCtrl, nil).Once()
	// Only the two convertible messages reach the controller, and the second
	// of them fails.
	publishErr := errors.New("broker rejected the message")
	s.mockCtrl.On("Publish", mock.Anything, "test-topic", mock.Anything).
		Run(func(args mock.Arguments) {
			s.Require().Len(args.Get(2).([]kafka.BrokerMessage), 2, "unconvertible messages are not sent")
		}).
		Return([]error{nil, publishErr}, nil).Once()

	broker := s.newConnectedBroker(ctx)

	msgs := []any{
		nil, // no message
		PublishMessage{Headers: PublishHeaders{CorrelationID: "cid-ok"}},
		make(chan int), // conversion error
		PublishMessage{Headers: PublishHeaders{CorrelationID: "cid-fail"}},
	}

	err := broker.PublishBatch(ctx, "test-topic", msgs)
	s.Require().Error(err, "a partial failure must be reported")

	results, ok := kafka.BatchErrors(err)
	s.Require().True(ok, "a partial failure must carry per-message errors")
	s.Require().Len(results, len(msgs), "one result per message")

	s.Require().Error(results[0], "nil message must be reported")
	s.Require().NoError(results[1], "convertible message must be published")
	s.Require().Error(results[2], "unconvertible message must be reported")
	s.Require().ErrorIs(results[3], publishErr, "publish error must land on its own message")
}

func (s *PublishSuite) TestPublishBatch_GenericTypedSlice() {
	var captured []kafka.BrokerMessage
	s.mockFactory.On("New", mock.Anything).
		Return(s.mockCtrl, nil).Once()
	s.mockCtrl.On("Publish", mock.Anything, "test-topic", mock.Anything).
		Run(func(args mock.Arguments) {
			captured = args.Get(2).([]kafka.BrokerMessage)
		}).
		Return([]error{nil, nil}, nil).Once()

	broker := s.newConnectedBroker(s.T().Context())

	err := broker.PublishBatch(s.T().Context(), "test-topic", []string{"first", "second"})
	s.Require().NoError(err, "publish generic typed batch")
	s.Require().Len(captured, 2, "each slice element must become one broker message")
	s.Require().Equal([]byte(`"first"`), captured[0].Payload, "first payload mismatch")
	s.Require().Equal([]byte(`"second"`), captured[1].Payload, "second payload mismatch")
}

func (s *PublishSuite) TestPublishBatch_Fail() {
	ctx := context.Background()

	batchErr := errors.New("whole batch failed")
	s.mockFactory.On("New", mock.Anything).
		Return(s.mockCtrl, nil).Once()
	s.mockCtrl.On("Publish", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, batchErr).Once()

	broker := s.newConnectedBroker(ctx)

	err := broker.PublishBatch(ctx, "test-topic", []any{
		PublishMessage{Headers: PublishHeaders{CorrelationID: "cid"}},
	})
	s.Require().ErrorIs(err, batchErr, "batch error must reach the caller")

	_, ok := kafka.BatchErrors(err)
	s.Require().False(ok, "a whole-batch failure carries no per-message errors")
}

func (s *PublishSuite) TestPublishBatch_Empty() {
	ctx := context.Background()

	s.mockFactory.On("New", mock.Anything).
		Return(s.mockCtrl, nil).Once()

	broker := s.newConnectedBroker(ctx)

	err := broker.PublishBatch(ctx, "test-topic", []any{})
	s.Require().NoError(err, "empty batch is not an error")
}

func (s *PublishSuite) TestPublishBatch_NotConnected() {
	ctx := context.Background()

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

	err = broker.PublishBatch(ctx, "test-topic", []any{
		PublishMessage{Headers: PublishHeaders{CorrelationID: "cid"}},
	})
	s.Require().Error(err, "publishing without Connect must fail")
}

func (s *PublishSuite) TestPublishBatch_InvalidInput() {
	testCases := []struct {
		messages any
		name     string
	}{
		{name: "nil", messages: nil},
		{name: "struct", messages: PublishMessage{}},
		{name: "pointer to slice", messages: &[]PublishMessage{}},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			s.mockFactory.On("New", mock.Anything).
				Return(s.mockCtrl, nil).Once()

			broker := s.newConnectedBroker(s.T().Context())

			err := broker.PublishBatch(s.T().Context(), "test-topic", tC.messages)
			s.Require().Error(err, "non-slice batch input must fail")
		})
	}
}

// endregion
