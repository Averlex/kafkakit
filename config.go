package kafka

import (
	"time"

	"github.com/IBM/sarama"
)

// Mode is a preferred usage mode: producer-only, consumer-only or full client (mixed).
// Either choice affects only the list of required options.
type Mode = string

const (
	// ModeProducer is used when the broker is only used to publish messages.
	// When this mode is set, consumer-specific options like GroupID are not required.
	ModeProducer Mode = "producer"

	// ModeConsumer is used when the broker is only used to subscribe to messages.
	// When this mode is set, GroupID option is required for consumer group management.
	ModeConsumer Mode = "consumer"

	// ModeMixed is used when the broker is used for both publishing and subscribing.
	// This is the default mode. When this mode is set, GroupID option is required
	// for any subscription operations.
	ModeMixed Mode = "mixed"
)

const (
	// DefaultMode is the default broker usage mode.
	DefaultMode Mode = ModeMixed
	// DefaultMaxBytes is the default maximum Kafka message size.
	DefaultMaxBytes = 10 * 1024 * 1024
	// DefaultTopicCreateAttempts disables automatic topic creation by default.
	DefaultTopicCreateAttempts = 0
	// DefaultTopicCreateBackoff is the default delay between topic creation attempts.
	DefaultTopicCreateBackoff = 100 * time.Millisecond
	// DefaultBatchSize is the default number of messages buffered in a producer batch.
	DefaultBatchSize = 100
	// DefaultBatchBytes is the default producer batch size limit in bytes.
	DefaultBatchBytes int64 = 1024 * 1024
	// DefaultBatchTimeout is the default maximum wait for an incomplete producer batch.
	DefaultBatchTimeout = 10 * time.Millisecond
	// DefaultNackBackoff is the default delay before retrying a negatively acknowledged message.
	DefaultNackBackoff = time.Second
	// DefaultAutoCommit disables consumer offset auto commit by default.
	DefaultAutoCommit = false
	// DefaultConnectionTest enables the connection check by default.
	DefaultConnectionTest = true
)

// Config is an internal structure for storing parameters.
type Config struct {
	ControllerFactory ControllerFactory // Factory for creating Kafka controllers; used for testing.
	SaramaFactory     SaramaFactory     // Factory for constructing Sarama clients; used for testing and customization.

	DSN string `json:"dsn" yaml:"dsn" env:"KAFKA_DSN"`
	// Consumer group ID, required for consumer. No defaults.
	GroupID string `json:"group_id" yaml:"group_id" env:"KAFKA_GROUP_ID"`
	// Default - ModeMixed.
	Mode Mode `json:"mode" yaml:"mode" env:"KAFKA_MODE"`
	// Broker addresses. When empty, DSN is used as the only address.
	Hosts []string `json:"hosts" yaml:"hosts" env:"KAFKA_HOSTS" env-separator:","`
	// Max message size (default - 10MB). Must be less than Sarama's
	// maximum request size.
	MaxBytes int `json:"max_bytes" yaml:"max_bytes" env:"KAFKA_MAX_BYTES"`
	// How many times a missing topic is created before publishing fails. The
	// default is 0, which disables automatic topic creation.
	TopicCreateAttempts int `json:"topic_create_attempts" yaml:"topic_create_attempts" env:"KAFKA_TOPIC_CREATE_ATTEMPTS"`
	// Delay between topic creation attempts, letting the cluster propagate
	// the new topic metadata (default - 100ms).
	TopicCreateBackoff time.Duration `json:"topic_create_backoff" yaml:"topic_create_backoff" env:"KAFKA_TOPIC_CREATE_BACKOFF"` //nolint:lll
	// How many messages a producer buffers before sending them as one batch
	// (default - 100).
	BatchSize int `json:"batch_size" yaml:"batch_size" env:"KAFKA_BATCH_SIZE"`
	// Max size of a batch in bytes (default - 1MB). Must be less than
	// Sarama's maximum request size.
	BatchBytes int64 `json:"batch_bytes" yaml:"batch_bytes" env:"KAFKA_BATCH_BYTES"`
	// How long an incomplete batch waits before being flushed anyway
	// (default - 10ms).
	BatchTimeout time.Duration `json:"batch_timeout" yaml:"batch_timeout" env:"KAFKA_BATCH_TIMEOUT"`
	// Delay before retrying a message whose handler returned an error
	// (default - 1s).
	NackBackoff time.Duration `json:"nack_backoff" yaml:"nack_backoff" env:"KAFKA_NACK_BACKOFF"`
	// Offset auto commit (default - false).
	AutoCommit bool `json:"auto_commit" yaml:"auto_commit" env:"KAFKA_AUTO_COMMIT"`
	// Default - true.
	ConnectionTest bool `json:"connection_test" yaml:"connection_test" env:"KAFKA_CONNECTION_TEST"`
}

// DefaultConfig returns the default config.
func DefaultConfig() Config {
	return Config{
		DSN:                 "",
		Hosts:               nil,
		GroupID:             "",
		MaxBytes:            DefaultMaxBytes,
		AutoCommit:          DefaultAutoCommit,
		ConnectionTest:      DefaultConnectionTest,
		Mode:                DefaultMode,
		TopicCreateAttempts: DefaultTopicCreateAttempts,
		TopicCreateBackoff:  DefaultTopicCreateBackoff,
		BatchSize:           DefaultBatchSize,
		BatchBytes:          DefaultBatchBytes,
		BatchTimeout:        DefaultBatchTimeout,
		NackBackoff:         DefaultNackBackoff,
		ControllerFactory:   &defaultControllerFactory{},
		SaramaFactory:       &defaultSaramaFactory{},
	}
}

// brokerHosts returns the addresses to connect to: the explicit host list when
// it is set, the DSN otherwise.
func (c Config) brokerHosts() []string {
	if len(c.Hosts) > 0 {
		return c.Hosts
	}

	return []string{c.DSN}
}

// buildProducerConfig maps the producer settings to Sarama without changing
// the configured values. validate ensures that all values fit Sarama's limits.
func buildProducerConfig(cfg Config) *sarama.Config {
	producerConfig := buildSaramaConfig()
	producerConfig.Metadata.AllowAutoTopicCreation = false
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producerConfig.Producer.Return.Errors = true
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.Flush.Messages = cfg.BatchSize
	producerConfig.Producer.Flush.MaxMessages = cfg.BatchSize
	producerConfig.Producer.Flush.Frequency = cfg.BatchTimeout
	producerConfig.Producer.Flush.Bytes = int(cfg.BatchBytes)
	producerConfig.Producer.MaxMessageBytes = cfg.MaxBytes

	return producerConfig
}

// buildConsumerConfig maps consumer settings to Sarama.
func buildConsumerConfig(cfg Config) *sarama.Config {
	consumerConfig := buildSaramaConfig()
	consumerConfig.Metadata.AllowAutoTopicCreation = false
	consumerConfig.Consumer.Fetch.Max = int32(cfg.MaxBytes)
	consumerConfig.Consumer.Offsets.AutoCommit.Enable = cfg.AutoCommit
	consumerConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()

	return consumerConfig
}

func buildSaramaConfig() *sarama.Config {
	return sarama.NewConfig()
}
