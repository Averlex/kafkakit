package kafka

import "github.com/IBM/sarama"

// SaramaFactory constructs the Sarama clients used by the Kafka controller.
// Applications normally use the default implementation. The interface allows
// deterministic adapter tests and specialized client construction.
type SaramaFactory interface {
	NewClient(hosts []string, cfg *sarama.Config) (sarama.Client, error)
	NewAsyncProducer(hosts []string, cfg *sarama.Config) (sarama.AsyncProducer, error)
	NewClusterAdmin(hosts []string, cfg *sarama.Config) (sarama.ClusterAdmin, error)
	NewConsumerGroup(hosts []string, groupID string, cfg *sarama.Config) (sarama.ConsumerGroup, error)
}

type defaultSaramaFactory struct{}

// NewClient constructs a Sarama client.
func (f *defaultSaramaFactory) NewClient(hosts []string, cfg *sarama.Config) (sarama.Client, error) {
	return sarama.NewClient(hosts, cfg)
}

// NewAsyncProducer constructs a Sarama asynchronous producer.
func (f *defaultSaramaFactory) NewAsyncProducer(
	hosts []string,
	cfg *sarama.Config,
) (sarama.AsyncProducer, error) {
	return sarama.NewAsyncProducer(hosts, cfg)
}

// NewClusterAdmin constructs a Sarama cluster administrator.
func (f *defaultSaramaFactory) NewClusterAdmin(
	hosts []string,
	cfg *sarama.Config,
) (sarama.ClusterAdmin, error) {
	return sarama.NewClusterAdmin(hosts, cfg)
}

// NewConsumerGroup constructs a Sarama consumer group.
func (f *defaultSaramaFactory) NewConsumerGroup(
	hosts []string,
	groupID string,
	cfg *sarama.Config,
) (sarama.ConsumerGroup, error) {
	return sarama.NewConsumerGroup(hosts, groupID, cfg)
}

// defaultControllerFactory creates real Kafka controllers.
type defaultControllerFactory struct{}

// New creates a Sarama-backed controller.
func (dcf *defaultControllerFactory) New(cfg Config) (Controller, error) {
	hosts := cfg.brokerHosts()
	if cfg.ConnectionTest {
		client, err := cfg.SaramaFactory.NewClient(hosts, buildConsumerConfig(cfg))
		if err != nil {
			return nil, err
		}
		if err := client.Close(); err != nil {
			return nil, err
		}
	}

	writer := newControllerWriter(cfg, func() (asyncProducer, error) {
		return cfg.SaramaFactory.NewAsyncProducer(hosts, buildProducerConfig(cfg))
	}, func() (*topicManager, error) {
		admin, err := cfg.SaramaFactory.NewClusterAdmin(hosts, buildProducerConfig(cfg))
		if err != nil {
			return nil, err
		}

		return newSaramaTopicManager(admin), nil
	})

	consumerFactory := func() (*consumerGroup, error) {
		group, err := cfg.SaramaFactory.NewConsumerGroup(hosts, cfg.GroupID, buildConsumerConfig(cfg))
		if err != nil {
			return nil, err
		}

		return newSaramaConsumerGroup(group, cfg.AutoCommit), nil
	}

	return NewControllerAdapter(writer, consumerFactory, cfg.NackBackoff), nil
}
