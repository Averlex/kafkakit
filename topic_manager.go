package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

type topicManager struct {
	createTopicFn    func(string, *sarama.TopicDetail, bool) error
	describeTopicsFn func([]string) ([]*sarama.TopicMetadata, error)
	closeFn          func() error
}

type topicManagerFactory func() (*topicManager, error)

func newSaramaTopicManager(admin sarama.ClusterAdmin) *topicManager {
	return &topicManager{
		createTopicFn:    admin.CreateTopic,
		describeTopicsFn: admin.DescribeTopics,
		closeFn:          admin.Close,
	}
}

func (m *topicManager) createTopic(topic string, detail *sarama.TopicDetail) error {
	return m.createTopicFn(topic, detail, false)
}

func (m *topicManager) describeTopics(topics []string) ([]*sarama.TopicMetadata, error) {
	return m.describeTopicsFn(topics)
}

func (m *topicManager) close() error {
	return m.closeFn()
}

// createTopic creates a missing topic and waits for usable partition metadata.
// Calls are serialized so concurrent publishers do not race to create it.
func (c *ControllerWriter) createTopic(ctx context.Context, topic string) error {
	c.topicManagerMu.Lock()
	defer c.topicManagerMu.Unlock()

	if err := ctx.Err(); err != nil {
		return context.Cause(ctx)
	}
	if err := c.initTopicManager(); err != nil {
		return fmt.Errorf("create topic manager: %w", err)
	}

	detail := &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}
	for attempt := range c.topicCreateAttempts {
		err := c.topicManager.createTopic(topic, detail)
		if err != nil && !errors.Is(err, sarama.ErrTopicAlreadyExists) {
			return err
		}

		available, err := c.topicAvailable(topic)
		if err != nil {
			return err
		}
		if available {
			return nil
		}

		if attempt < c.topicCreateAttempts-1 {
			if err := waitForTopicRetry(ctx, c.topicCreateBackoff); err != nil {
				return err
			}
		}
	}

	return fmt.Errorf("topic %s is not available after %d attempts", topic, c.topicCreateAttempts)
}

// initTopicManager lazily creates the admin client after topic creation has
// been explicitly enabled and a publish has reported a missing topic.
func (c *ControllerWriter) initTopicManager() error {
	if c.topicManager != nil {
		return nil
	}
	if c.topicManagerFactory == nil {
		return errors.New("topic manager factory is nil")
	}

	manager, err := c.topicManagerFactory()
	if err != nil {
		return err
	}
	if manager == nil {
		return errors.New("topic manager factory returned nil")
	}

	c.topicManager = manager

	return nil
}

// topicAvailable reports whether the topic has usable partition metadata.
// Metadata propagation errors are retried by createTopic; permanent errors are
// returned immediately.
func (c *ControllerWriter) topicAvailable(topic string) (bool, error) {
	metadata, err := c.topicManager.describeTopics([]string{topic})
	if err != nil {
		return false, err
	}

	for _, topicMetadata := range metadata {
		if topicMetadata == nil || topicMetadata.Name != topic {
			continue
		}

		switch topicMetadata.Err {
		case sarama.ErrNoError:
			return len(topicMetadata.Partitions) > 0, nil
		case sarama.ErrUnknownTopicOrPartition, sarama.ErrLeaderNotAvailable, sarama.ErrReplicaNotAvailable:
			return false, nil
		default:
			return false, topicMetadata.Err
		}
	}

	return false, nil
}

// waitForTopicRetry waits for metadata propagation without ignoring caller
// cancellation.
func waitForTopicRetry(ctx context.Context, backoff time.Duration) error {
	timer := time.NewTimer(backoff)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
