package kafka

// BrokerMessage is the Kafka library's transport representation.
// Application code normally uses Publish and FromBrokerMessage instead.
type BrokerMessage struct {
	Headers map[string][]byte
	Payload []byte
}

// AcknowledgeableBrokerMessage is a transport message with delivery control.
// It is exposed for controller and subscription test adapters; application
// handlers receive a *BrokerMessage as an opaque any value.
type AcknowledgeableBrokerMessage struct {
	BrokerMessage
	acknowledgment messageAcknowledgment
}

type messageAcknowledgment interface {
	AckMessage()
	NakMessage()
}

// NewAcknowledgeableBrokerMessage attaches delivery control to a broker
// message. It is intended for custom controller and subscription adapters.
func NewAcknowledgeableBrokerMessage(
	message BrokerMessage,
	acknowledgment messageAcknowledgment,
) AcknowledgeableBrokerMessage {
	return AcknowledgeableBrokerMessage{
		BrokerMessage:  message,
		acknowledgment: acknowledgment,
	}
}

// Ack acknowledges the delivery when acknowledgment control is present.
func (m AcknowledgeableBrokerMessage) Ack() {
	if m.acknowledgment != nil {
		m.acknowledgment.AckMessage()
	}
}

// Nak rejects the delivery when acknowledgment control is present.
func (m AcknowledgeableBrokerMessage) Nak() {
	if m.acknowledgment != nil {
		m.acknowledgment.NakMessage()
	}
}
