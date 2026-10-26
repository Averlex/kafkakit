package kafka_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/Averlex/kafkakit"
)

// region Suite

func TestFromBrokerMessageSuite(t *testing.T) {
	suite.Run(t, new(FromBrokerMessageSuite))
}

type FromBrokerMessageSuite struct {
	suite.Suite
	marshal func(any) []byte
}

func (s *FromBrokerMessageSuite) SetupTest() {
	s.marshal = func(v any) []byte {
		b, err := json.Marshal(v)
		if err != nil {
			s.Require().NoError(err, "marshal failed")
		}
		return b
	}
}

// endregion

// region Structs

type TestHeaders struct {
	CorrelationID string       `json:"correlation_id"`
	Tags          []string     `json:"tags,omitempty"`
	Metadata      TestMetadata `json:"metadata,omitempty"`
}

type TestMetadata struct {
	Version int `json:"version"`
}

type TestPayload struct {
	Action string            `json:"action"`
	Items  []int             `json:"items"`
	Labels map[string]string `json:"labels"`
	Nested TestNested        `json:"nested"`
}

type TestNested struct {
	ID   string `json:"id"`
	Data []byte `json:"data"`
}

type HeadersWithIgnored struct {
	CorrelationID string `json:"correlation_id"`
	Ignored       string `json:"-"`
}

type NumericHeaders struct {
	Priority int     `json:"priority"`
	Enabled  bool    `json:"enabled"`
	Ratio    float64 `json:"ratio"`
}

type PayloadWithNestedPointer struct {
	Action string `json:"action"`
	Meta   *struct {
		ID string `json:"id"`
	} `json:"meta"`
}

type MessageValueHeaders struct {
	Headers TestHeaders
	Payload TestPayload
}

type MessagePointerHeaders struct {
	Headers *TestHeaders
	Payload TestPayload
}

type MessageIgnoredHeaders struct {
	Headers HeadersWithIgnored
	Payload struct{ Action string }
}

type MessageNumericHeaders struct {
	Headers NumericHeaders
	Payload struct{ Action string }
}

type MessageNestedPointer struct {
	Headers TestHeaders
	Payload PayloadWithNestedPointer
}

type MessageMapPayload struct {
	Headers TestHeaders
	Payload struct {
		Extras map[string]int `json:"extras"`
	}
}

type GenericMessage struct {
	Action string         `json:"action"`
	Items  []int          `json:"items"`
	Meta   map[string]any `json:"meta"`
}

type GenericMessageWithEnvelopeNames struct {
	Headers string `json:"headers"`
	Payload int    `json:"payload"`
}

// endregion

// region Tests

func (s *FromBrokerMessageSuite) TestFromBrokerMessage_Valid() {
	testCases := []struct {
		name     string
		msg      *kafka.BrokerMessage
		target   any
		expected any
	}{
		{
			name: "full message",
			msg: &kafka.BrokerMessage{
				Headers: map[string][]byte{
					"correlation_id": s.marshal("req-1"),
					"tags":           s.marshal([]string{"a", "b"}),
					"metadata":       s.marshal(TestMetadata{Version: 2}),
				},
				Payload: s.marshal(TestPayload{
					Action: "create",
					Items:  []int{1, 2, 3},
					Labels: map[string]string{"env": "test"},
					Nested: TestNested{ID: "x1", Data: []byte("secret")},
				}),
			},
			target: &MessageValueHeaders{},
			expected: &MessageValueHeaders{
				Headers: TestHeaders{
					CorrelationID: "req-1",
					Tags:          []string{"a", "b"},
					Metadata:      TestMetadata{Version: 2},
				},
				Payload: TestPayload{
					Action: "create",
					Items:  []int{1, 2, 3},
					Labels: map[string]string{"env": "test"},
					Nested: TestNested{ID: "x1", Data: []byte("secret")},
				},
			},
		},
		{
			name: "no headers",
			msg: &kafka.BrokerMessage{
				Headers: nil,
				Payload: s.marshal(TestPayload{
					Action: "no-headers",
					Items:  []int{},
					Labels: map[string]string{},
					Nested: TestNested{},
				}),
			},
			target: &MessageValueHeaders{},
			expected: &MessageValueHeaders{
				Headers: TestHeaders{},
				Payload: TestPayload{
					Action: "no-headers",
					Items:  []int{},
					Labels: map[string]string{},
					Nested: TestNested{},
				},
			},
		},
		{
			name: "headers is nil pointer",
			msg: &kafka.BrokerMessage{
				Headers: map[string][]byte{
					"correlation_id": s.marshal("req-2"),
				},
				Payload: s.marshal(TestPayload{
					Action: "update",
					Items:  []int{},
					Labels: map[string]string{},
					Nested: TestNested{ID: "x2", Data: []byte("")},
				}),
			},
			target: &MessagePointerHeaders{},
			expected: &MessagePointerHeaders{
				Headers: &TestHeaders{CorrelationID: "req-2"},
				Payload: TestPayload{
					Action: "update",
					Items:  []int{},
					Labels: map[string]string{},
					Nested: TestNested{ID: "x2", Data: []byte("")},
				},
			},
		},
		{
			name: "headers is non-nil pointer",
			msg: &kafka.BrokerMessage{
				Headers: map[string][]byte{
					"correlation_id": s.marshal("req-3"),
				},
				Payload: s.marshal(TestPayload{
					Action: "noop",
					Items:  []int{},
					Labels: map[string]string{},
					Nested: TestNested{},
				}),
			},
			target: &MessagePointerHeaders{
				Headers: &TestHeaders{},
			},
			expected: &MessagePointerHeaders{
				Headers: &TestHeaders{CorrelationID: "req-3"},
				Payload: TestPayload{
					Action: "noop",
					Items:  []int{},
					Labels: map[string]string{},
					Nested: TestNested{},
				},
			},
		},
		{
			name: "headers with ignored field",
			msg: &kafka.BrokerMessage{
				Headers: map[string][]byte{
					"correlation_id": s.marshal("req-4"),
				},
				Payload: s.marshal(struct{ Action string }{Action: "test"}),
			},
			target: &MessageIgnoredHeaders{},
			expected: &MessageIgnoredHeaders{
				Headers: HeadersWithIgnored{CorrelationID: "req-4"},
				Payload: struct{ Action string }{Action: "test"},
			},
		},
		{
			name: "headers with numeric and bool",
			msg: &kafka.BrokerMessage{
				Headers: map[string][]byte{
					"priority": s.marshal(5),
					"enabled":  s.marshal(true),
					"ratio":    s.marshal(0.75),
				},
				Payload: s.marshal(struct{ Action string }{Action: "numeric"}),
			},
			target: &MessageNumericHeaders{},
			expected: &MessageNumericHeaders{
				Headers: NumericHeaders{Priority: 5, Enabled: true, Ratio: 0.75},
				Payload: struct{ Action string }{Action: "numeric"},
			},
		},
		{
			name: "missing optional header",
			msg: &kafka.BrokerMessage{
				Headers: map[string][]byte{
					"correlation_id": s.marshal("req-5"),
				},
				Payload: s.marshal(TestPayload{
					Action: "partial",
					Items:  []int{},
					Labels: map[string]string{},
					Nested: TestNested{},
				}),
			},
			target: &MessageValueHeaders{},
			expected: &MessageValueHeaders{
				Headers: TestHeaders{CorrelationID: "req-5"},
				Payload: TestPayload{
					Action: "partial",
					Items:  []int{},
					Labels: map[string]string{},
					Nested: TestNested{},
				},
			},
		},
		{
			name: "payload with nested pointer",
			msg: &kafka.BrokerMessage{
				Headers: map[string][]byte{
					"correlation_id": s.marshal("req-6"),
				},
				Payload: s.marshal(PayloadWithNestedPointer{
					Action: "nested-ptr",
					Meta: &struct {
						ID string `json:"id"`
					}{ID: "m1"},
				}),
			},
			target: &MessageNestedPointer{},
			expected: &MessageNestedPointer{
				Headers: TestHeaders{CorrelationID: "req-6"},
				Payload: PayloadWithNestedPointer{
					Action: "nested-ptr",
					Meta: &struct {
						ID string `json:"id"`
					}{ID: "m1"},
				},
			},
		},
		{
			name: "empty slices and maps",
			msg: &kafka.BrokerMessage{
				Headers: map[string][]byte{
					"correlation_id": s.marshal("req-7"),
					"tags":           s.marshal([]string{}),
				},
				Payload: s.marshal(TestPayload{
					Action: "empty",
					Items:  []int{},
					Labels: map[string]string{},
					Nested: TestNested{ID: "", Data: nil},
				}),
			},
			target: &MessageValueHeaders{},
			expected: &MessageValueHeaders{
				Headers: TestHeaders{CorrelationID: "req-7", Tags: []string{}},
				Payload: TestPayload{
					Action: "empty",
					Items:  []int{},
					Labels: map[string]string{},
					Nested: TestNested{ID: "", Data: nil},
				},
			},
		},
		{
			name: "payload with map",
			msg: &kafka.BrokerMessage{
				Headers: map[string][]byte{
					"correlation_id": s.marshal("req-8"),
				},
				Payload: s.marshal(struct {
					Extras map[string]int `json:"extras"`
				}{Extras: map[string]int{"a": 1, "b": 2}}),
			},
			target: &MessageMapPayload{},
			expected: &MessageMapPayload{
				Headers: TestHeaders{CorrelationID: "req-8"},
				Payload: struct {
					Extras map[string]int `json:"extras"`
				}{Extras: map[string]int{"a": 1, "b": 2}},
			},
		},
		{
			name: "generic struct",
			msg: &kafka.BrokerMessage{
				Payload: s.marshal(GenericMessage{
					Action: "generic",
					Items:  []int{1, 2},
					Meta:   map[string]any{"enabled": true},
				}),
			},
			target: &GenericMessage{},
			expected: &GenericMessage{
				Action: "generic",
				Items:  []int{1, 2},
				Meta:   map[string]any{"enabled": true},
			},
		},
		{
			name: "generic slice",
			msg: &kafka.BrokerMessage{
				Payload: s.marshal([]string{"first", "second"}),
			},
			target:   &[]string{},
			expected: &[]string{"first", "second"},
		},
		{
			name: "raw bytes",
			msg: &kafka.BrokerMessage{
				Payload: []byte{0xff, 0x00, 0x7f},
			},
			target:   &[]byte{},
			expected: &[]byte{0xff, 0x00, 0x7f},
		},
		{
			name:     "nil raw bytes",
			msg:      &kafka.BrokerMessage{},
			target:   new([]byte),
			expected: new([]byte),
		},
		{
			name: "envelope field names with generic header",
			msg: &kafka.BrokerMessage{
				Payload: s.marshal(GenericMessageWithEnvelopeNames{Headers: "metadata", Payload: 42}),
			},
			target:   &GenericMessageWithEnvelopeNames{},
			expected: &GenericMessageWithEnvelopeNames{Headers: "metadata", Payload: 42},
		},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			err := kafka.FromBrokerMessage(tC.msg, tC.target)
			require.NoError(s.T(), err, "decode broker message")
			require.Equal(s.T(), tC.expected, tC.target, "decoded message mismatch")
		})
	}
}

func (s *FromBrokerMessageSuite) TestFromBrokerMessage_Fail() {
	testCases := []struct {
		name   string
		msg    any
		target any
	}{
		{
			name:   "nil broker message",
			msg:    (*kafka.BrokerMessage)(nil),
			target: &MessageValueHeaders{},
		},
		{
			name:   "non pointer target",
			msg:    &kafka.BrokerMessage{Payload: s.marshal(struct{}{})},
			target: MessageValueHeaders{},
		},
		{
			name:   "nil pointer target",
			msg:    &kafka.BrokerMessage{Payload: s.marshal(struct{}{})},
			target: (*MessageValueHeaders)(nil),
		},
		{
			name: "invalid payload JSON",
			msg: &kafka.BrokerMessage{
				Payload: []byte("{invalid}"),
			},
			target: &MessageValueHeaders{},
		},
		{
			name: "invalid header JSON",
			msg: &kafka.BrokerMessage{
				Headers: map[string][]byte{
					"correlation_id": []byte("{invalid}"),
				},
				Payload: s.marshal(TestPayload{
					Action: "test",
					Items:  []int{},
					Labels: map[string]string{},
					Nested: TestNested{},
				}),
			},
			target: &MessageValueHeaders{},
		},
		{
			name:   "wrong message type",
			msg:    "not a broker message",
			target: &MessageValueHeaders{},
		},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			err := kafka.FromBrokerMessage(tC.msg, tC.target)
			require.Error(s.T(), err, "invalid decode must fail")
		})
	}
}

// endregion
