package kafka_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/Averlex/kafkakit"
)

// region Suite

type SaramaAdapterSuite struct {
	suite.Suite
}

func TestSaramaAdapterSuite(t *testing.T) {
	suite.Run(t, new(SaramaAdapterSuite))
}

// endregion

// region Stubs

const adapterTestTopic = "adapter-test-topic"

type saramaFactoryStub struct {
	newClient        func([]string, *sarama.Config) (sarama.Client, error)
	newProducer      func([]string, *sarama.Config) (sarama.AsyncProducer, error)
	newAdmin         func([]string, *sarama.Config) (sarama.ClusterAdmin, error)
	newConsumerGroup func([]string, string, *sarama.Config) (sarama.ConsumerGroup, error)
}

func (f *saramaFactoryStub) NewClient(hosts []string, cfg *sarama.Config) (sarama.Client, error) {
	if f.newClient == nil {
		return nil, errors.New("unexpected client creation")
	}

	return f.newClient(hosts, cfg)
}

func (f *saramaFactoryStub) NewAsyncProducer(
	hosts []string,
	cfg *sarama.Config,
) (sarama.AsyncProducer, error) {
	if f.newProducer == nil {
		return nil, errors.New("unexpected producer creation")
	}

	return f.newProducer(hosts, cfg)
}

func (f *saramaFactoryStub) NewClusterAdmin(
	hosts []string,
	cfg *sarama.Config,
) (sarama.ClusterAdmin, error) {
	if f.newAdmin == nil {
		return nil, errors.New("unexpected administrator creation")
	}

	return f.newAdmin(hosts, cfg)
}

func (f *saramaFactoryStub) NewConsumerGroup(
	hosts []string,
	groupID string,
	cfg *sarama.Config,
) (sarama.ConsumerGroup, error) {
	if f.newConsumerGroup == nil {
		return nil, errors.New("unexpected consumer group creation")
	}

	return f.newConsumerGroup(hosts, groupID, cfg)
}

type saramaClientStub struct {
	sarama.Client
	closeErr error
	closed   atomic.Int32
}

func (c *saramaClientStub) Close() error {
	c.closed.Add(1)

	return c.closeErr
}

type asyncProducerStub struct {
	sarama.AsyncProducer

	input     chan *sarama.ProducerMessage
	successes chan *sarama.ProducerMessage
	errors    chan *sarama.ProducerError
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
	respond   func(int, *sarama.ProducerMessage) error

	mu       sync.Mutex
	messages []*sarama.ProducerMessage
}

func newAsyncProducerStub(respond func(int, *sarama.ProducerMessage) error) *asyncProducerStub {
	producer := &asyncProducerStub{
		input:     make(chan *sarama.ProducerMessage),
		successes: make(chan *sarama.ProducerMessage),
		errors:    make(chan *sarama.ProducerError),
		done:      make(chan struct{}),
		respond:   respond,
	}
	go producer.run()

	return producer
}

func (p *asyncProducerStub) run() {
	defer close(p.done)
	defer close(p.successes)
	defer close(p.errors)

	index := 0
	for message := range p.input {
		p.mu.Lock()
		p.messages = append(p.messages, message)
		p.mu.Unlock()

		var err error
		if p.respond != nil {
			err = p.respond(index, message)
		}
		index++

		if err != nil {
			p.errors <- &sarama.ProducerError{Msg: message, Err: err}
			continue
		}
		p.successes <- message
	}
}

func (p *asyncProducerStub) Input() chan<- *sarama.ProducerMessage {
	return p.input
}

func (p *asyncProducerStub) Successes() <-chan *sarama.ProducerMessage {
	return p.successes
}

func (p *asyncProducerStub) Errors() <-chan *sarama.ProducerError {
	return p.errors
}

func (p *asyncProducerStub) Close() error {
	p.closeOnce.Do(func() {
		close(p.input)
		<-p.done
	})

	return p.closeErr
}

func (p *asyncProducerStub) snapshot() []*sarama.ProducerMessage {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]*sarama.ProducerMessage(nil), p.messages...)
}

type clusterAdminStub struct {
	sarama.ClusterAdmin
	createTopic    func(string, *sarama.TopicDetail, bool) error
	describeTopics func([]string) ([]*sarama.TopicMetadata, error)
	closeErr       error
	closed         atomic.Int32
}

func (a *clusterAdminStub) CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
	return a.createTopic(topic, detail, validateOnly)
}

func (a *clusterAdminStub) DescribeTopics(topics []string) ([]*sarama.TopicMetadata, error) {
	return a.describeTopics(topics)
}

func (a *clusterAdminStub) Close() error {
	a.closed.Add(1)

	return a.closeErr
}

type consumerGroupStub struct {
	sarama.ConsumerGroup
	consume  func(context.Context, []string, sarama.ConsumerGroupHandler) error
	closeErr error
	closed   atomic.Int32
}

func (g *consumerGroupStub) Consume(
	ctx context.Context,
	topics []string,
	handler sarama.ConsumerGroupHandler,
) error {
	return g.consume(ctx, topics, handler)
}

func (g *consumerGroupStub) Close() error {
	g.closed.Add(1)

	return g.closeErr
}

type consumerSessionStub struct {
	sarama.ConsumerGroupSession
	ctx       context.Context
	marked    chan *sarama.ConsumerMessage
	committed atomic.Int32
}

func (s *consumerSessionStub) Context() context.Context {
	return s.ctx
}

func (s *consumerSessionStub) MarkMessage(message *sarama.ConsumerMessage, _ string) {
	s.marked <- message
}

func (s *consumerSessionStub) Commit() {
	s.committed.Add(1)
}

type consumerClaimStub struct {
	sarama.ConsumerGroupClaim
	messages <-chan *sarama.ConsumerMessage
}

func (c *consumerClaimStub) Messages() <-chan *sarama.ConsumerMessage {
	return c.messages
}

func adapterConfig(factory kafka.SaramaFactory, mode kafka.Mode) kafka.Config {
	cfg := kafka.DefaultConfig()
	cfg.DSN = testDSN
	cfg.GroupID = testGroupID
	cfg.Mode = mode
	cfg.ConnectionTest = false
	cfg.SaramaFactory = factory

	return cfg
}

func connectedAdapterBroker(t *testing.T, cfg kafka.Config) *kafka.Broker {
	t.Helper()

	broker, err := kafka.NewBroker(&cfg)
	require.NoError(t, err, "create adapter-backed broker")
	require.NotNil(t, broker, "adapter-backed broker must be returned")
	require.NoError(t, broker.Connect(t.Context()), "connect adapter-backed broker")

	return broker
}

// region Producer

func (s *SaramaAdapterSuite) TestPublishAndProducerConfiguration() {
	t := s.T()

	publishErr := errors.New("message rejected")
	producer := newAsyncProducerStub(func(index int, _ *sarama.ProducerMessage) error {
		if index == 1 {
			return publishErr
		}

		return nil
	})
	var capturedHosts []string
	var capturedConfig *sarama.Config
	factory := &saramaFactoryStub{
		newProducer: func(hosts []string, cfg *sarama.Config) (sarama.AsyncProducer, error) {
			capturedHosts = append([]string(nil), hosts...)
			capturedConfig = cfg

			return producer, nil
		},
	}
	cfg := adapterConfig(factory, kafka.ModeProducer)
	cfg.Hosts = []string{"broker-a:9092", "broker-b:9092"}
	cfg.BatchSize = 25
	cfg.BatchBytes = 512 * 1024
	cfg.BatchTimeout = 25 * time.Millisecond
	cfg.MaxBytes = 8 * 1024 * 1024
	broker := connectedAdapterBroker(t, cfg)

	err := broker.PublishBatch(t.Context(), adapterTestTopic, []string{"first", "second", "third"})
	require.Error(t, err, "partial producer failure must be reported")
	results, ok := kafka.BatchErrors(err)
	require.True(t, ok, "partial producer failure must expose batch results")
	require.Len(t, results, 3, "one producer result must exist per message")
	require.NoError(t, results[0], "first message must succeed")
	require.ErrorIs(t, results[1], publishErr, "second message must retain the producer error")
	require.NoError(t, results[2], "third message must succeed")

	broker.Close(t.Context())
	require.Equal(t, cfg.Hosts, capturedHosts, "explicit broker hosts must be passed to Sarama")
	require.NotNil(t, capturedConfig, "producer configuration must be captured")
	require.False(t, capturedConfig.Metadata.AllowAutoTopicCreation, "producer must disable implicit topic creation")
	require.Equal(t, sarama.WaitForAll, capturedConfig.Producer.RequiredAcks, "producer acknowledgment mode mismatch")
	require.True(t, capturedConfig.Producer.Return.Errors, "producer errors must be enabled")
	require.True(t, capturedConfig.Producer.Return.Successes, "producer successes must be enabled")
	require.Equal(t, cfg.BatchSize, capturedConfig.Producer.Flush.Messages, "producer batch size mismatch")
	require.Equal(t, cfg.BatchSize, capturedConfig.Producer.Flush.MaxMessages, "producer batch limit mismatch")
	require.Equal(t, int(cfg.BatchBytes), capturedConfig.Producer.Flush.Bytes, "producer batch byte limit mismatch")
	require.Equal(t, cfg.BatchTimeout, capturedConfig.Producer.Flush.Frequency, "producer batch timeout mismatch")
	require.Equal(t, cfg.MaxBytes, capturedConfig.Producer.MaxMessageBytes, "producer message size mismatch")
	require.NoError(t, capturedConfig.Validate(), "captured producer configuration must be valid")

	messages := producer.snapshot()
	require.Len(t, messages, 3, "producer must receive every batch message")
	require.Equal(t, adapterTestTopic, messages[0].Topic, "producer topic mismatch")
	payload, err := messages[0].Value.Encode()
	require.NoError(t, err, "encode captured producer payload")
	require.Equal(t, []byte(`"first"`), payload, "producer payload mismatch")
}

func (s *SaramaAdapterSuite) TestProducerInitializationErrorAndCancellation() {
	t := s.T()

	producerErr := errors.New("producer unavailable")
	var producerCalls atomic.Int32
	factory := &saramaFactoryStub{
		newProducer: func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
			producerCalls.Add(1)

			return nil, producerErr
		},
	}
	broker := connectedAdapterBroker(t, adapterConfig(factory, kafka.ModeProducer))

	err := broker.Publish(t.Context(), adapterTestTopic, "message")
	require.ErrorIs(t, err, producerErr, "producer initialization error must reach the caller")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = broker.Publish(ctx, adapterTestTopic, "message")
	require.ErrorIs(t, err, context.Canceled, "cancelled publish must report context cancellation")
	require.Equal(t, int32(1), producerCalls.Load(), "cancelled publish must not retry producer initialization")

	broker.Close(t.Context())
}

func (s *SaramaAdapterSuite) TestPublishAfterClose() {
	t := s.T()

	producer := newAsyncProducerStub(nil)
	factory := &saramaFactoryStub{
		newProducer: func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
			return producer, nil
		},
	}
	broker := connectedAdapterBroker(t, adapterConfig(factory, kafka.ModeProducer))
	require.NoError(t, broker.Publish(t.Context(), adapterTestTopic, "before close"), "initial publish must succeed")
	broker.Close(t.Context())

	err := broker.Publish(t.Context(), adapterTestTopic, "after close")
	require.Error(t, err, "broker must reject publication after close")
}

// endregion

// region Topic Creation

func (s *SaramaAdapterSuite) TestCreatesMissingTopicAndRetries() {
	t := s.T()

	producer := newAsyncProducerStub(func(index int, _ *sarama.ProducerMessage) error {
		if index == 0 {
			return sarama.ErrUnknownTopicOrPartition
		}

		return nil
	})
	var createCalls atomic.Int32
	var describeCalls atomic.Int32
	admin := &clusterAdminStub{
		createTopic: func(topic string, detail *sarama.TopicDetail, validateOnly bool) error {
			createCalls.Add(1)
			require.Equal(t, adapterTestTopic, topic, "created topic mismatch")
			require.Equal(t, int32(1), detail.NumPartitions, "created topic partition count mismatch")
			require.Equal(t, int16(1), detail.ReplicationFactor, "created topic replication factor mismatch")
			require.False(t, validateOnly, "topic creation must not be validation-only")

			return &sarama.TopicError{Err: sarama.ErrTopicAlreadyExists}
		},
		describeTopics: func(topics []string) ([]*sarama.TopicMetadata, error) {
			describeCalls.Add(1)
			require.Equal(t, []string{adapterTestTopic}, topics, "described topic mismatch")

			return []*sarama.TopicMetadata{{
				Name:       adapterTestTopic,
				Err:        sarama.ErrNoError,
				Partitions: []*sarama.PartitionMetadata{{ID: 0}},
			}}, nil
		},
	}
	factory := &saramaFactoryStub{
		newProducer: func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
			return producer, nil
		},
		newAdmin: func([]string, *sarama.Config) (sarama.ClusterAdmin, error) {
			return admin, nil
		},
	}
	cfg := adapterConfig(factory, kafka.ModeProducer)
	cfg.TopicCreateAttempts = 1
	broker := connectedAdapterBroker(t, cfg)

	require.NoError(t, broker.Publish(t.Context(), adapterTestTopic, "message"), "publish must retry after creating topic")
	broker.Close(t.Context())
	require.Equal(t, int32(1), createCalls.Load(), "topic must be created once")
	require.Equal(t, int32(1), describeCalls.Load(), "topic metadata must be read once")
	require.Equal(t, int32(1), admin.closed.Load(), "topic administrator must be closed")
	require.Len(t, producer.snapshot(), 2, "message must be submitted again after topic creation")
}

func (s *SaramaAdapterSuite) TestDoesNotCreateMissingTopicWhenDisabled() {
	t := s.T()

	producer := newAsyncProducerStub(func(int, *sarama.ProducerMessage) error {
		return sarama.ErrUnknownTopicOrPartition
	})
	var adminCalls atomic.Int32
	factory := &saramaFactoryStub{
		newProducer: func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
			return producer, nil
		},
		newAdmin: func([]string, *sarama.Config) (sarama.ClusterAdmin, error) {
			adminCalls.Add(1)

			return nil, errors.New("topic creation must remain disabled")
		},
	}
	broker := connectedAdapterBroker(t, adapterConfig(factory, kafka.ModeProducer))

	err := broker.Publish(t.Context(), adapterTestTopic, "message")
	require.ErrorIs(t, err, sarama.ErrUnknownTopicOrPartition, "missing topic error must reach the caller")
	broker.Close(t.Context())
	require.Zero(t, adminCalls.Load(), "disabled topic creation must not construct an administrator")
}

func (s *SaramaAdapterSuite) TestTopicCreationFailures() {
	t := s.T()

	testErr := errors.New("topic operation failed")
	testCases := []struct {
		name        string
		attempts    int
		createTopic func(string, *sarama.TopicDetail, bool) error
		describe    func([]string) ([]*sarama.TopicMetadata, error)
		adminErr    error
	}{
		{
			name:     "administrator creation",
			attempts: 1,
			adminErr: testErr,
		},
		{
			name:     "topic creation",
			attempts: 1,
			createTopic: func(string, *sarama.TopicDetail, bool) error {
				return testErr
			},
			describe: func([]string) ([]*sarama.TopicMetadata, error) {
				return nil, nil
			},
		},
		{
			name:     "topic description",
			attempts: 1,
			createTopic: func(string, *sarama.TopicDetail, bool) error {
				return nil
			},
			describe: func([]string) ([]*sarama.TopicMetadata, error) {
				return nil, testErr
			},
		},
		{
			name:     "permanent metadata error",
			attempts: 1,
			createTopic: func(string, *sarama.TopicDetail, bool) error {
				return nil
			},
			describe: func([]string) ([]*sarama.TopicMetadata, error) {
				return []*sarama.TopicMetadata{{Name: adapterTestTopic, Err: sarama.ErrInvalidTopic}}, nil
			},
		},
		{
			name:     "metadata unavailable",
			attempts: 1,
			createTopic: func(string, *sarama.TopicDetail, bool) error {
				return nil
			},
			describe: func([]string) ([]*sarama.TopicMetadata, error) {
				return []*sarama.TopicMetadata{nil, {Name: "another-topic"}}, nil
			},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.name, func(t *testing.T) {
			producer := newAsyncProducerStub(func(int, *sarama.ProducerMessage) error {
				return sarama.ErrUnknownTopicOrPartition
			})
			factory := &saramaFactoryStub{
				newProducer: func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
					return producer, nil
				},
				newAdmin: func([]string, *sarama.Config) (sarama.ClusterAdmin, error) {
					if tC.adminErr != nil {
						return nil, tC.adminErr
					}

					return &clusterAdminStub{
						createTopic:    tC.createTopic,
						describeTopics: tC.describe,
					}, nil
				},
			}
			cfg := adapterConfig(factory, kafka.ModeProducer)
			cfg.TopicCreateAttempts = tC.attempts
			broker := connectedAdapterBroker(t, cfg)

			err := broker.Publish(t.Context(), adapterTestTopic, "message")
			require.Error(t, err, "topic creation failure must reach the caller")
			broker.Close(t.Context())
		})
	}
}

func (s *SaramaAdapterSuite) TestCancelsTopicCreationBackoff() {
	t := s.T()

	producer := newAsyncProducerStub(func(int, *sarama.ProducerMessage) error {
		return sarama.ErrUnknownTopicOrPartition
	})
	metadataChecked := make(chan struct{})
	var checkedOnce sync.Once
	admin := &clusterAdminStub{
		createTopic: func(string, *sarama.TopicDetail, bool) error {
			return nil
		},
		describeTopics: func([]string) ([]*sarama.TopicMetadata, error) {
			checkedOnce.Do(func() { close(metadataChecked) })

			return []*sarama.TopicMetadata{{
				Name: adapterTestTopic,
				Err:  sarama.ErrLeaderNotAvailable,
			}}, nil
		},
	}
	factory := &saramaFactoryStub{
		newProducer: func([]string, *sarama.Config) (sarama.AsyncProducer, error) {
			return producer, nil
		},
		newAdmin: func([]string, *sarama.Config) (sarama.ClusterAdmin, error) {
			return admin, nil
		},
	}
	cfg := adapterConfig(factory, kafka.ModeProducer)
	cfg.TopicCreateAttempts = 2
	cfg.TopicCreateBackoff = time.Hour
	broker := connectedAdapterBroker(t, cfg)
	ctx, cancel := context.WithCancel(t.Context())
	completed := make(chan error, 1)
	go func() {
		completed <- broker.Publish(ctx, adapterTestTopic, "message")
	}()

	select {
	case <-metadataChecked:
	case <-time.After(time.Second):
		require.FailNow(t, "topic metadata must be checked", "timed out waiting for topic metadata")
	}
	cancel()
	select {
	case err := <-completed:
		require.ErrorIs(t, err, context.Canceled, "topic retry backoff must honor cancellation")
	case <-time.After(time.Second):
		require.FailNow(t, "publish must stop", "timed out waiting for cancelled topic retry")
	}
	broker.Close(t.Context())
}

// endregion

// region Consumer

func (s *SaramaAdapterSuite) TestSubscribeRetriesNackAndCommitsAck() {
	t := s.T()

	expectedMaxBytes := kafka.DefaultMaxBytes
	marked := make(chan *sarama.ConsumerMessage, 1)
	session := &consumerSessionStub{ctx: t.Context(), marked: marked}
	message := &sarama.ConsumerMessage{
		Topic: adapterTestTopic,
		Value: []byte(`{"id":"message-1"}`),
		Headers: []*sarama.RecordHeader{
			nil,
			{Key: []byte("correlation_id"), Value: []byte(`"correlation-1"`)},
		},
	}
	group := &consumerGroupStub{}
	group.consume = func(
		ctx context.Context,
		topics []string,
		handler sarama.ConsumerGroupHandler,
	) error {
		require.Equal(t, []string{adapterTestTopic}, topics, "consumer topics mismatch")
		session.ctx = ctx
		require.NoError(t, handler.Setup(session), "consumer handler setup must succeed")
		messages := make(chan *sarama.ConsumerMessage, 2)
		messages <- nil
		messages <- message
		close(messages)
		require.NoError(t, handler.ConsumeClaim(session, &consumerClaimStub{messages: messages}), "consume claim")
		require.NoError(t, handler.Cleanup(session), "consumer handler cleanup must succeed")

		return errors.New("test consumption complete")
	}
	factory := &saramaFactoryStub{
		newConsumerGroup: func(hosts []string, groupID string, cfg *sarama.Config) (sarama.ConsumerGroup, error) {
			require.Equal(t, []string{testDSN}, hosts, "consumer DSN mismatch")
			require.Equal(t, testGroupID, groupID, "consumer group ID mismatch")
			require.False(t, cfg.Metadata.AllowAutoTopicCreation, "consumer must disable implicit topic creation")
			require.Equal(t, int32(expectedMaxBytes), cfg.Consumer.Fetch.Max, "consumer fetch size mismatch")
			require.False(t, cfg.Consumer.Offsets.AutoCommit.Enable, "consumer auto commit mismatch")
			require.Equal(t, sarama.RangeBalanceStrategyName, cfg.Consumer.Group.Rebalance.Strategy.Name(), "consumer strategy mismatch")

			return group, nil
		},
	}
	cfg := adapterConfig(factory, kafka.ModeConsumer)
	cfg.NackBackoff = time.Millisecond
	broker := connectedAdapterBroker(t, cfg)
	var deliveries atomic.Int32
	done, err := broker.Subscribe(t.Context(), adapterTestTopic, func(_ context.Context, raw any, _ func(any, any) error) error {
		brokerMessage, ok := raw.(*kafka.BrokerMessage)
		require.True(t, ok, "handler must receive a broker message")
		require.Equal(t, message.Value, brokerMessage.Payload, "consumed payload mismatch")
		require.Equal(t, []byte(`"correlation-1"`), brokerMessage.Headers["correlation_id"], "consumed header mismatch")
		if deliveries.Add(1) == 1 {
			return errors.New("retry message")
		}

		return nil
	})
	require.NoError(t, err, "subscribe through Sarama adapter")

	select {
	case <-done:
	case <-time.After(time.Second):
		require.FailNow(t, "subscription must finish", "timed out waiting for subscription")
	}
	select {
	case committed := <-marked:
		require.Same(t, message, committed, "acknowledged message mismatch")
	default:
		require.Fail(t, "message must be marked", "acknowledged message was not marked")
	}
	require.Equal(t, int32(2), deliveries.Load(), "nacked message must be delivered again")
	require.Equal(t, int32(1), session.committed.Load(), "manual commit must happen after acknowledgment")
	require.Equal(t, int32(1), group.closed.Load(), "consumer group must be closed")
	broker.Close(t.Context())
}

func (s *SaramaAdapterSuite) TestSubscribeAutoCommitAndEmptyHeaders() {
	t := s.T()

	marked := make(chan *sarama.ConsumerMessage, 1)
	session := &consumerSessionStub{ctx: t.Context(), marked: marked}
	message := &sarama.ConsumerMessage{Topic: adapterTestTopic, Value: []byte("raw")}
	group := &consumerGroupStub{}
	group.consume = func(ctx context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
		session.ctx = ctx
		messages := make(chan *sarama.ConsumerMessage, 1)
		messages <- message
		close(messages)

		require.NoError(t, handler.ConsumeClaim(session, &consumerClaimStub{messages: messages}), "consume claim")

		return errors.New("test consumption complete")
	}
	factory := &saramaFactoryStub{
		newConsumerGroup: func([]string, string, *sarama.Config) (sarama.ConsumerGroup, error) {
			return group, nil
		},
	}
	cfg := adapterConfig(factory, kafka.ModeConsumer)
	cfg.AutoCommit = true
	broker := connectedAdapterBroker(t, cfg)
	done, err := broker.Subscribe(t.Context(), adapterTestTopic, func(_ context.Context, raw any, _ func(any, any) error) error {
		brokerMessage, ok := raw.(*kafka.BrokerMessage)
		require.True(t, ok, "handler must receive a broker message")
		require.Nil(t, brokerMessage.Headers, "headerless message must retain nil headers")

		return nil
	})
	require.NoError(t, err, "subscribe with auto commit")

	select {
	case <-marked:
	case <-time.After(time.Second):
		require.FailNow(t, "message must be marked", "timed out waiting for message acknowledgment")
	}
	require.Zero(t, session.committed.Load(), "auto commit mode must not commit synchronously")
	broker.Close(t.Context())
	select {
	case <-done:
	case <-time.After(time.Second):
		require.FailNow(t, "subscription must stop", "timed out waiting for subscription shutdown")
	}
}

func (s *SaramaAdapterSuite) TestUnsubscribeInterruptsNackBackoff() {
	t := s.T()

	messageHandled := make(chan struct{})
	session := &consumerSessionStub{marked: make(chan *sarama.ConsumerMessage, 1)}
	group := &consumerGroupStub{}
	group.consume = func(ctx context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
		session.ctx = ctx
		messages := make(chan *sarama.ConsumerMessage, 1)
		messages <- &sarama.ConsumerMessage{Topic: adapterTestTopic, Value: []byte("raw")}
		close(messages)

		return handler.ConsumeClaim(session, &consumerClaimStub{messages: messages})
	}
	factory := &saramaFactoryStub{
		newConsumerGroup: func([]string, string, *sarama.Config) (sarama.ConsumerGroup, error) {
			return group, nil
		},
	}
	cfg := adapterConfig(factory, kafka.ModeConsumer)
	cfg.NackBackoff = time.Hour
	broker := connectedAdapterBroker(t, cfg)
	done, err := broker.Subscribe(t.Context(), adapterTestTopic, func(context.Context, any, func(any, any) error) error {
		close(messageHandled)

		return errors.New("retry later")
	})
	require.NoError(t, err, "subscribe before cancellation")

	select {
	case <-messageHandled:
	case <-time.After(time.Second):
		require.FailNow(t, "handler must run", "timed out waiting for handler")
	}
	cancelCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, broker.Unsubscribe(cancelCtx, adapterTestTopic), "unsubscribe during nack backoff")
	select {
	case <-done:
	case <-time.After(time.Second):
		require.FailNow(t, "subscription must stop", "timed out waiting for cancelled subscription")
	}
	require.Zero(t, session.committed.Load(), "nacked message must not be committed")
	broker.Close(t.Context())
}

func (s *SaramaAdapterSuite) TestConsumerGroupCreationError() {
	t := s.T()

	groupErr := errors.New("consumer group unavailable")
	factory := &saramaFactoryStub{
		newConsumerGroup: func([]string, string, *sarama.Config) (sarama.ConsumerGroup, error) {
			return nil, groupErr
		},
	}
	broker := connectedAdapterBroker(t, adapterConfig(factory, kafka.ModeConsumer))

	done, err := broker.Subscribe(t.Context(), adapterTestTopic, func(context.Context, any, func(any, any) error) error {
		return nil
	})
	require.ErrorIs(t, err, groupErr, "consumer group error must reach the caller")
	select {
	case <-done:
	default:
		require.Fail(t, "failed subscription completion channel must be closed")
	}
	broker.Close(t.Context())
}

// endregion

// region Connection

func (s *SaramaAdapterSuite) TestConnectionTest() {
	t := s.T()

	client := &saramaClientStub{}
	var capturedHosts []string
	var capturedConfig *sarama.Config
	factory := &saramaFactoryStub{
		newClient: func(hosts []string, cfg *sarama.Config) (sarama.Client, error) {
			capturedHosts = append([]string(nil), hosts...)
			capturedConfig = cfg

			return client, nil
		},
	}
	cfg := adapterConfig(factory, kafka.ModeProducer)
	cfg.ConnectionTest = true
	broker := connectedAdapterBroker(t, cfg)
	broker.Close(t.Context())

	require.Equal(t, []string{testDSN}, capturedHosts, "DSN must be used when explicit hosts are absent")
	require.NotNil(t, capturedConfig, "connection test configuration must be captured")
	require.Equal(t, int32(cfg.MaxBytes), capturedConfig.Consumer.Fetch.Max, "consumer fetch limit mismatch")
	require.Equal(t, int32(1), client.closed.Load(), "connection test client must be closed")
}

func (s *SaramaAdapterSuite) TestConnectionTestFailures() {
	t := s.T()

	testErr := errors.New("connection test failed")
	testCases := []struct {
		name   string
		client sarama.Client
		err    error
	}{
		{name: "client creation", err: testErr},
		{name: "client close", client: &saramaClientStub{closeErr: testErr}},
	}

	for _, tC := range testCases {
		t.Run(tC.name, func(t *testing.T) {
			factory := &saramaFactoryStub{
				newClient: func([]string, *sarama.Config) (sarama.Client, error) {
					return tC.client, tC.err
				},
			}
			cfg := adapterConfig(factory, kafka.ModeProducer)
			cfg.ConnectionTest = true
			broker, err := kafka.NewBroker(&cfg)
			require.NoError(t, err, "create broker for connection failure")

			err = broker.Connect(t.Context())
			require.ErrorIs(t, err, testErr, "connection test failure must reach the caller")
		})
	}
}

// endregion
