package kafka_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/Averlex/kafkakit"
)

// region Suite

type AsyncPublishSuite struct {
	suite.Suite
}

func TestAsyncPublishSuite(t *testing.T) {
	suite.Run(t, new(AsyncPublishSuite))
}

// endregion

// region Single Publication

func (s *AsyncPublishSuite) TestPublishAsync_CompletesAfterBrokerResult() {
	t := s.T()

	producerStarted := make(chan struct{})
	releaseProducer := make(chan struct{})
	producer := newAsyncProducerStub(func(int, *sarama.ProducerMessage) error {
		close(producerStarted)
		<-releaseProducer

		return nil
	})
	factory := &saramaFactoryStub{
		newProducer: func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
			return producer, nil
		},
	}
	broker := connectedAdapterBroker(t, adapterConfig(factory, kafka.ModeProducer))

	result := broker.PublishAsync(t.Context(), adapterTestTopic, "message")
	select {
	case <-producerStarted:
	case <-time.After(time.Second):
		require.FailNow(t, "producer must receive message", "timed out waiting for producer submission")
	}
	select {
	case <-result.Done():
		require.Fail(t, "result must remain pending", "publication completed before the producer responded")
	default:
	}

	close(releaseProducer)
	select {
	case <-result.Done():
	case <-time.After(time.Second):
		require.FailNow(t, "result must complete", "timed out waiting for publication result")
	}
	require.NoError(t, result.Err(), "successful asynchronous publication must have no error")
	broker.Close(t.Context())
}

func (s *AsyncPublishSuite) TestPublishAsync_ImmediateFailure() {
	t := s.T()

	broker, err := kafka.NewBroker(&kafka.Config{
		DSN:      testDSN,
		Mode:     kafka.ModeProducer,
		MaxBytes: testMaxBytes,
	})
	require.NoError(t, err, "create disconnected broker")

	result := broker.PublishAsync(t.Context(), adapterTestTopic, "message")
	select {
	case <-result.Done():
	default:
		require.Fail(t, "immediate failure must be complete", "disconnected publication result remained pending")
	}
	require.Error(t, result.Err(), "disconnected asynchronous publication must fail")
}

func (s *AsyncPublishSuite) TestPublishAsync_CancellationCompletesPendingResult() {
	t := s.T()

	producerStarted := make(chan struct{})
	releaseProducer := make(chan struct{})
	producer := newAsyncProducerStub(func(int, *sarama.ProducerMessage) error {
		close(producerStarted)
		<-releaseProducer

		return nil
	})
	factory := &saramaFactoryStub{
		newProducer: func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
			return producer, nil
		},
	}
	broker := connectedAdapterBroker(t, adapterConfig(factory, kafka.ModeProducer))
	ctx, cancel := context.WithCancel(t.Context())
	result := broker.PublishAsync(ctx, adapterTestTopic, "message")
	select {
	case <-producerStarted:
	case <-time.After(time.Second):
		require.FailNow(t, "producer must receive message", "timed out waiting for producer submission")
	}

	cancel()
	select {
	case <-result.Done():
	case <-time.After(time.Second):
		require.FailNow(t, "cancelled result must complete", "timed out waiting for cancellation")
	}
	require.ErrorIs(t, result.Err(), context.Canceled, "cancelled publication must retain the context error")

	close(releaseProducer)
	broker.Close(t.Context())
}

func (s *AsyncPublishSuite) TestPublishAsync_CloseWaitsForPendingPublication() {
	t := s.T()

	producerStarted := make(chan struct{})
	releaseProducer := make(chan struct{})
	producer := newAsyncProducerStub(func(int, *sarama.ProducerMessage) error {
		close(producerStarted)
		<-releaseProducer

		return nil
	})
	factory := &saramaFactoryStub{
		newProducer: func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
			return producer, nil
		},
	}
	broker := connectedAdapterBroker(t, adapterConfig(factory, kafka.ModeProducer))
	result := broker.PublishAsync(t.Context(), adapterTestTopic, "message")
	select {
	case <-producerStarted:
	case <-time.After(time.Second):
		require.FailNow(t, "producer must receive message", "timed out waiting for producer submission")
	}
	closed := make(chan struct{})
	ctx := t.Context()
	go func() {
		broker.Close(ctx)
		close(closed)
	}()

	select {
	case <-closed:
		require.Fail(t, "close must wait", "broker closed before the pending publication completed")
	default:
	}
	close(releaseProducer)
	select {
	case <-closed:
	case <-time.After(time.Second):
		require.FailNow(t, "broker must close", "timed out waiting for pending publication during close")
	}
	select {
	case <-result.Done():
	case <-time.After(time.Second):
		require.FailNow(t, "result must complete", "pending publication did not complete during close")
	}
	require.NoError(t, result.Err(), "publication completed during close must succeed")
}

// endregion

// region Batch Publication

func (s *AsyncPublishSuite) TestPublishBatchAsync_PartialFailure() {
	t := s.T()

	publishErr := errors.New("message rejected")
	producer := newAsyncProducerStub(func(index int, _ *sarama.ProducerMessage) error {
		if index == 1 {
			return publishErr
		}

		return nil
	})
	factory := &saramaFactoryStub{
		newProducer: func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
			return producer, nil
		},
	}
	broker := connectedAdapterBroker(t, adapterConfig(factory, kafka.ModeProducer))

	result := broker.PublishBatchAsync(
		t.Context(),
		adapterTestTopic,
		[]string{"first", "second", "third"},
	)
	select {
	case <-result.Done():
	case <-time.After(time.Second):
		require.FailNow(t, "batch result must complete", "timed out waiting for batch publication")
	}
	require.Error(t, result.Err(), "partial batch failure must set the overall error")
	messageErrors := result.MessageErrors()
	require.Len(t, messageErrors, 3, "batch result must contain one outcome per message")
	require.NoError(t, messageErrors[0], "first message must succeed")
	require.ErrorIs(t, messageErrors[1], publishErr, "second message must retain its producer error")
	require.NoError(t, messageErrors[2], "third message must succeed")
	require.Len(t, producer.snapshot(), 3, "a failed message must not prevent later submissions")

	messageErrors[0] = publishErr
	require.NoError(t, result.MessageErrors()[0], "callers must not mutate stored batch outcomes")
	broker.Close(t.Context())
}

func (s *AsyncPublishSuite) TestPublishBatchAsync_ConversionFailureDoesNotStopValidMessages() {
	t := s.T()

	producer := newAsyncProducerStub(nil)
	factory := &saramaFactoryStub{
		newProducer: func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
			return producer, nil
		},
	}
	broker := connectedAdapterBroker(t, adapterConfig(factory, kafka.ModeProducer))

	result := broker.PublishBatchAsync(
		t.Context(),
		adapterTestTopic,
		[]any{"first", make(chan int), "third"},
	)
	<-result.Done()
	require.Error(t, result.Err(), "conversion failure must set the overall error")
	messageErrors := result.MessageErrors()
	require.Len(t, messageErrors, 3, "conversion outcomes must remain aligned")
	require.NoError(t, messageErrors[0], "first message must succeed")
	require.Error(t, messageErrors[1], "unencodable message must fail")
	require.NoError(t, messageErrors[2], "third message must still succeed")
	require.Len(t, producer.snapshot(), 2, "all encodable messages must be submitted")
	broker.Close(t.Context())
}

func (s *AsyncPublishSuite) TestPublishBatchAsync_OperationFailureHasNoMessageOutcomes() {
	t := s.T()

	producerErr := errors.New("producer unavailable")
	factory := &saramaFactoryStub{
		newProducer: func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
			return nil, producerErr
		},
	}
	broker := connectedAdapterBroker(t, adapterConfig(factory, kafka.ModeProducer))

	result := broker.PublishBatchAsync(t.Context(), adapterTestTopic, []string{"first", "second"})
	<-result.Done()
	require.ErrorIs(t, result.Err(), producerErr, "operation failure must retain its cause")
	require.Nil(t, result.MessageErrors(), "operation failure must not claim individual outcomes")
	broker.Close(t.Context())
}

func (s *AsyncPublishSuite) TestPublishBatchAsync_EmptyBatch() {
	t := s.T()

	factory := &saramaFactoryStub{}
	broker := connectedAdapterBroker(t, adapterConfig(factory, kafka.ModeProducer))

	result := broker.PublishBatchAsync(t.Context(), adapterTestTopic, []string{})
	select {
	case <-result.Done():
	default:
		require.Fail(t, "empty batch must complete immediately", "empty batch result remained pending")
	}
	require.NoError(t, result.Err(), "empty batch must succeed")
	messageErrors := result.MessageErrors()
	require.NotNil(t, messageErrors, "empty batch must have available individual outcomes")
	require.Empty(t, messageErrors, "empty batch must have no individual errors")
	broker.Close(t.Context())
}

func (s *AsyncPublishSuite) TestPublishBatchAsync_InvalidInput() {
	t := s.T()

	factory := &saramaFactoryStub{}
	broker := connectedAdapterBroker(t, adapterConfig(factory, kafka.ModeProducer))

	result := broker.PublishBatchAsync(t.Context(), adapterTestTopic, "not a slice")
	select {
	case <-result.Done():
	default:
		require.Fail(t, "invalid batch must complete immediately", "invalid batch result remained pending")
	}
	require.Error(t, result.Err(), "invalid asynchronous batch must fail")
	require.Nil(t, result.MessageErrors(), "invalid batch cannot have individual outcomes")
	broker.Close(t.Context())
}

func (s *AsyncPublishSuite) TestPublishBatchAsync_ConcurrentResults() {
	t := s.T()

	const publisherCount = 25

	producer := newAsyncProducerStub(nil)
	factory := &saramaFactoryStub{
		newProducer: func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
			return producer, nil
		},
	}
	broker := connectedAdapterBroker(t, adapterConfig(factory, kafka.ModeProducer))
	results := make(chan kafka.BatchPublishResult, publisherCount)
	ctx := t.Context()
	var publishers sync.WaitGroup
	for index := range publisherCount {
		publishers.Add(1)
		go func(value int) {
			defer publishers.Done()
			results <- broker.PublishBatchAsync(ctx, adapterTestTopic, []int{value, value + 1})
		}(index)
	}
	publishers.Wait()
	close(results)

	for result := range results {
		select {
		case <-result.Done():
		case <-time.After(time.Second):
			require.FailNow(t, "concurrent result must complete", "timed out waiting for concurrent batch")
		}
		require.NoError(t, result.Err(), "concurrent batch must succeed")
		require.Equal(t, []error{nil, nil}, result.MessageErrors(), "concurrent batch outcomes mismatch")
	}
	broker.Close(t.Context())
}

// endregion
