package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

const (
	envelopeHeadersFieldName = "Headers"
	envelopePayloadFieldName = "Payload"
)

// FromBrokerMessage deserializes a message received by a Subscribe handler into
// target. Target must be a non-nil pointer.
//
// JSON is used for arbitrary values. A byte slice receives the raw Kafka
// payload. An envelope message with exported Headers and Payload fields decodes
// the payload and Kafka headers separately.
func FromBrokerMessage(msg any, target any) error {
	brokerMessage, ok := msg.(*BrokerMessage)
	if !ok {
		return errors.New("invalid message: this function must be called only inside a subscription handler")
	}

	if brokerMessage == nil {
		return errors.New("broker message is nil")
	}

	targetValue := reflect.ValueOf(target)
	if targetValue.Kind() != reflect.Ptr || targetValue.IsNil() {
		return errors.New("target must be a non-nil pointer")
	}

	dereferencedTarget := targetValue.Elem()
	if headersField, payloadField, envelope := envelopeMessageFields(dereferencedTarget); envelope {
		return decodeEnvelopeMessage(brokerMessage, headersField, payloadField)
	}

	if isByteSlice(dereferencedTarget.Type()) {
		if brokerMessage.Payload == nil {
			dereferencedTarget.Set(reflect.Zero(dereferencedTarget.Type()))

			return nil
		}

		payload := reflect.MakeSlice(dereferencedTarget.Type(), len(brokerMessage.Payload), len(brokerMessage.Payload))
		reflect.Copy(payload, reflect.ValueOf(brokerMessage.Payload))
		dereferencedTarget.Set(payload)

		return nil
	}

	if err := json.Unmarshal(brokerMessage.Payload, target); err != nil {
		return fmt.Errorf("unmarshal message: %w", err)
	}

	return nil
}

// toBrokerMessage converts any JSON-serializable value to a broker message.
// Byte slices are used as raw payloads. Envelope messages retain their
// Headers/Payload wire representation.
func toBrokerMessage(v any) (*BrokerMessage, error) {
	value := reflect.ValueOf(v)
	if value.IsValid() && isByteSlice(value.Type()) {
		if value.IsNil() {
			return &BrokerMessage{}, nil
		}

		payload := make([]byte, value.Len())
		reflect.Copy(reflect.ValueOf(payload), value)

		return &BrokerMessage{Payload: payload}, nil
	}

	if headersField, payloadField, envelope := envelopeMessageFields(indirectValue(value)); envelope {
		return encodeEnvelopeMessage(headersField, payloadField)
	}

	payload, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json marshal message: %w", err)
	}

	return &BrokerMessage{Payload: payload}, nil
}

// encodeEnvelopeMessage converts the envelope fields to Kafka headers and payload.
func encodeEnvelopeMessage(headersField reflect.Value, payloadField reflect.Value) (*BrokerMessage, error) {
	payload, err := json.Marshal(payloadField.Interface())
	if err != nil {
		return nil, fmt.Errorf("json marshal payload: %w", err)
	}

	headers := make(map[string][]byte)
	headersValue := indirectValue(headersField)
	if !headersValue.IsValid() {
		return &BrokerMessage{Headers: headers, Payload: payload}, nil
	}

	headersType := headersValue.Type()
	for i := 0; i < headersValue.NumField(); i++ {
		field := headersValue.Field(i)
		fieldType := headersType.Field(i)

		name, included := headerName(fieldType)
		if !included || !field.CanInterface() {
			continue
		}

		value, err := json.Marshal(field.Interface())
		if err != nil {
			return nil, fmt.Errorf("json marshal header %q: %w", name, err)
		}

		headers[name] = value
	}

	return &BrokerMessage{Headers: headers, Payload: payload}, nil
}

// decodeEnvelopeMessage decodes Kafka headers and payload into the envelope fields.
func decodeEnvelopeMessage(
	brokerMessage *BrokerMessage,
	headersField reflect.Value,
	payloadField reflect.Value,
) error {
	if err := json.Unmarshal(brokerMessage.Payload, payloadField.Addr().Interface()); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	headersValue := headersField
	for headersValue.Kind() == reflect.Ptr {
		if headersValue.IsNil() {
			headersValue.Set(reflect.New(headersValue.Type().Elem()))
		}
		headersValue = headersValue.Elem()
	}

	headersType := headersValue.Type()
	for i := 0; i < headersValue.NumField(); i++ {
		field := headersValue.Field(i)
		fieldType := headersType.Field(i)

		name, included := headerName(fieldType)
		if !included || !field.CanAddr() || !field.CanSet() {
			continue
		}

		header, exists := brokerMessage.Headers[name]
		if !exists {
			continue
		}

		if err := json.Unmarshal(header, field.Addr().Interface()); err != nil {
			return fmt.Errorf("unmarshal header %q: %w", name, err)
		}
	}

	return nil
}

// envelopeMessageFields identifies the full message envelope shape.
func envelopeMessageFields(value reflect.Value) (reflect.Value, reflect.Value, bool) {
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return reflect.Value{}, reflect.Value{}, false
	}

	valueType := value.Type()
	headersType, headersExist := valueType.FieldByName(envelopeHeadersFieldName)
	payloadType, payloadExists := valueType.FieldByName(envelopePayloadFieldName)
	if !headersExist || !payloadExists ||
		len(headersType.Index) != 1 || len(payloadType.Index) != 1 ||
		!headersType.IsExported() || !payloadType.IsExported() {
		return reflect.Value{}, reflect.Value{}, false
	}

	if !isStructOrStructPointer(headersType.Type) {
		return reflect.Value{}, reflect.Value{}, false
	}

	headersField := value.Field(headersType.Index[0])
	payloadField := value.Field(payloadType.Index[0])

	return headersField, payloadField, true
}

func indirectValue(value reflect.Value) reflect.Value {
	for value.IsValid() && value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}

	return value
}

func isStructOrStructPointer(typ reflect.Type) bool {
	for typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	return typ.Kind() == reflect.Struct
}

func isByteSlice(typ reflect.Type) bool {
	return typ.Kind() == reflect.Slice && typ.Elem() == reflect.TypeOf(byte(0))
}

func headerName(field reflect.StructField) (string, bool) {
	tag := field.Tag.Get("json")
	if tag == "" {
		return "", false
	}

	name := tag
	if commaIndex := strings.IndexByte(tag, ','); commaIndex != -1 {
		name = tag[:commaIndex]
	}

	return name, name != "-"
}
