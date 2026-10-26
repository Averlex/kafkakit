package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// asyncProducer is the part of Sarama's producer used by ControllerWriter.
type asyncProducer interface {
	Input() chan<- *sarama.ProducerMessage
	Successes() <-chan *sarama.ProducerMessage
	Errors() <-chan *sarama.ProducerError
	Close() error
}

type asyncProducerFactory func() (asyncProducer, error)

// ControllerWriter adapts Sarama's asynchronous producer to synchronous and
// asynchronous per-message publish results.
type ControllerWriter struct {
	producerFactory     asyncProducerFactory
	topicManagerFactory topicManagerFactory
	producer            asyncProducer
	topicManager        *topicManager
	done                chan struct{}

	topicCreateAttempts int
	topicCreateBackoff  time.Duration

	topicRetryCond   *sync.Cond
	topicRetryDone   chan struct{}
	topicRetryQueue  []topicRetryTask
	topicRetryClosed bool
	topicRetryMu     sync.Mutex

	mu             sync.RWMutex
	producerInitMu sync.Mutex
	topicManagerMu sync.Mutex
	active         sync.WaitGroup
	closed         bool
	closeErr       error
}

type producerReceipt struct {
	attempt *publishAttempt
	index   int
}

type topicRetryTask struct {
	ctx      context.Context
	complete func([]error, error)
	messages []BrokerMessage
	missing  []int
	results  []error
	topic    string
}

type publishAttempt struct {
	complete func([]error)
	stop     func() bool

	mu      sync.Mutex
	results []error
	settled []bool
	pending int
	done    bool
}

// newControllerWriter creates a writer whose producer and topic manager are
// initialized only when their first operation needs them.
func newControllerWriter(
	cfg Config,
	producerFactory asyncProducerFactory,
	topicManagerFactory topicManagerFactory,
) *ControllerWriter {
	writer := &ControllerWriter{
		producerFactory:     producerFactory,
		topicManagerFactory: topicManagerFactory,
		done:                make(chan struct{}),
		topicCreateAttempts: cfg.TopicCreateAttempts,
		topicCreateBackoff:  cfg.TopicCreateBackoff,
		topicRetryDone:      make(chan struct{}),
	}
	writer.topicRetryCond = sync.NewCond(&writer.topicRetryMu)
	go writer.runTopicRetries()

	return writer
}

// Publish submits all messages to the asynchronous producer and blocks until
// every submitted message succeeds, fails, or the context is cancelled.
func (c *ControllerWriter) Publish(
	ctx context.Context,
	topic string,
	messages []BrokerMessage,
) ([]error, error) {
	done := make(chan struct{})
	var results []error
	var publishErr error

	c.publishAsync(ctx, topic, messages, func(completed []error, err error) {
		results = completed
		publishErr = err
		close(done)
	})
	<-done

	return results, publishErr
}

// publishAsync submits messages and reports their final outcomes through the
// completion callback without dedicating a waiting goroutine to each call.
func (c *ControllerWriter) publishAsync(
	ctx context.Context,
	topic string,
	messages []BrokerMessage,
	complete func([]error, error),
) {
	if len(messages) == 0 {
		complete(nil, nil)
		return
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed {
		complete(nil, ErrWriterClosed)
		return
	}

	if err := ctx.Err(); err != nil {
		results := make([]error, len(messages))
		for i := range results {
			results[i] = err
		}

		complete(results, nil)
		return
	}

	if err := c.initProducer(); err != nil {
		complete(nil, fmt.Errorf("create async producer: %w", err))
		return
	}

	c.active.Add(1)
	finish := func(results []error, err error) {
		complete(results, err)
		c.active.Done()
	}

	c.publishOnceAsync(ctx, topic, messages, func(results []error) {
		missing := missingTopicIndexes(results)
		if len(missing) == 0 || c.topicCreateAttempts == 0 {
			finish(results, nil)
			return
		}

		c.enqueueTopicRetry(topicRetryTask{
			ctx:      ctx,
			topic:    topic,
			messages: messages,
			missing:  missing,
			results:  results,
			complete: finish,
		})
	})
}

func (c *ControllerWriter) enqueueTopicRetry(task topicRetryTask) {
	c.topicRetryMu.Lock()
	c.topicRetryQueue = append(c.topicRetryQueue, task)
	c.topicRetryCond.Signal()
	c.topicRetryMu.Unlock()
}

// runTopicRetries processes missing-topic recovery on one owned worker so a
// burst of failures cannot create an unbounded number of goroutines.
func (c *ControllerWriter) runTopicRetries() {
	defer close(c.topicRetryDone)

	for {
		c.topicRetryMu.Lock()
		for len(c.topicRetryQueue) == 0 && !c.topicRetryClosed {
			c.topicRetryCond.Wait()
		}
		if len(c.topicRetryQueue) == 0 {
			c.topicRetryMu.Unlock()

			return
		}
		task := c.topicRetryQueue[0]
		c.topicRetryQueue[0] = topicRetryTask{}
		c.topicRetryQueue = c.topicRetryQueue[1:]
		c.topicRetryMu.Unlock()

		c.createTopicAndRetry(
			task.ctx,
			task.topic,
			task.messages,
			task.missing,
			task.results,
			task.complete,
		)
	}
}

// closeTopicRetries stops and joins the topic-retry worker after all active
// publications have completed.
func (c *ControllerWriter) closeTopicRetries() {
	c.topicRetryMu.Lock()
	c.topicRetryClosed = true
	c.topicRetryCond.Broadcast()
	c.topicRetryMu.Unlock()
	<-c.topicRetryDone
}

// createTopicAndRetry creates a missing topic and resubmits only messages that
// failed with a missing-topic error.
func (c *ControllerWriter) createTopicAndRetry(
	ctx context.Context,
	topic string,
	messages []BrokerMessage,
	missing []int,
	results []error,
	complete func([]error, error),
) {
	if err := c.createTopic(ctx, topic); err != nil {
		complete(nil, fmt.Errorf("create topic %s: %w", topic, err))
		return
	}

	retryMessages := make([]BrokerMessage, len(missing))
	for i, index := range missing {
		retryMessages[i] = messages[index]
	}

	c.publishOnceAsync(ctx, topic, retryMessages, func(retryResults []error) {
		for i, index := range missing {
			results[index] = retryResults[i]
		}
		complete(results, nil)
	})
}

// initProducer creates the Sarama producer on the first Publish call. Keeping
// initialization lazy preserves ConnectionTest=false behavior and avoids a
// producer connection for consumer-only clients that never publish.
func (c *ControllerWriter) initProducer() error {
	c.producerInitMu.Lock()
	defer c.producerInitMu.Unlock()

	if c.producer != nil {
		return nil
	}
	if c.producerFactory == nil {
		return errors.New("async producer factory is nil")
	}

	producer, err := c.producerFactory()
	if err != nil {
		return err
	}
	if producer == nil {
		return errors.New("async producer factory returned nil")
	}

	c.producer = producer
	go c.dispatchResults(producer)

	return nil
}

// publishOnceAsync submits one attempt and reports aligned results after every
// message either receives a broker outcome or is cancelled.
func (c *ControllerWriter) publishOnceAsync(
	ctx context.Context,
	topic string,
	messages []BrokerMessage,
	complete func([]error),
) {
	attempt := newPublishAttempt(ctx, len(messages), complete)

	for i, message := range messages {
		if err := ctx.Err(); err != nil {
			attempt.resolve(i, err)
			continue
		}

		receipt := &producerReceipt{attempt: attempt, index: i}
		producerMessage := toSaramaMessage(topic, message, receipt)

		select {
		case c.producer.Input() <- producerMessage:
		case <-ctx.Done():
			attempt.resolve(i, context.Cause(ctx))
		}
	}
}

// newPublishAttempt creates an attempt whose unresolved results are completed
// when the caller context is cancelled.
func newPublishAttempt(
	ctx context.Context,
	messageCount int,
	complete func([]error),
) *publishAttempt {
	attempt := &publishAttempt{
		complete: complete,
		results:  make([]error, messageCount),
		settled:  make([]bool, messageCount),
		pending:  messageCount,
	}

	stop := context.AfterFunc(ctx, func() {
		attempt.cancel(context.Cause(ctx))
	})

	attempt.mu.Lock()
	if attempt.done {
		attempt.mu.Unlock()
		stop()
		return attempt
	}

	attempt.stop = stop
	attempt.mu.Unlock()
	return attempt
}

// resolve records one message outcome and completes the attempt when every
// message has settled.
func (a *publishAttempt) resolve(index int, err error) {
	a.mu.Lock()
	if a.done || a.settled[index] {
		a.mu.Unlock()
		return
	}

	a.results[index] = err
	a.settled[index] = true
	a.pending--
	if a.pending > 0 {
		a.mu.Unlock()
		return
	}

	a.finishLocked()
}

// cancel marks every unresolved message with the context error.
func (a *publishAttempt) cancel(err error) {
	a.mu.Lock()
	if a.done {
		a.mu.Unlock()
		return
	}

	for i := range a.results {
		if !a.settled[i] {
			a.results[i] = err
			a.settled[i] = true
		}
	}

	a.pending = 0
	a.finishLocked()
}

// finishLocked publishes an immutable result snapshot and releases the attempt
// mutex before invoking its completion callback.
func (a *publishAttempt) finishLocked() {
	a.done = true
	results := cloneErrors(a.results)
	complete := a.complete
	stop := a.stop
	a.mu.Unlock()

	if stop != nil {
		stop()
	}
	complete(results)
}

// dispatchResults continuously drains both Sarama result channels and routes
// each outcome to the Publish call that submitted the message.
func (c *ControllerWriter) dispatchResults(producer asyncProducer) {
	defer close(c.done)

	successes := producer.Successes()
	producerErrors := producer.Errors()

	for successes != nil || producerErrors != nil {
		select {
		case message, ok := <-successes:
			if !ok {
				successes = nil
				continue
			}
			completeProducerMessage(message, nil)

		case producerError, ok := <-producerErrors:
			if !ok {
				producerErrors = nil
				continue
			}
			if producerError == nil {
				continue
			}
			completeProducerMessage(producerError.Msg, producerError.Err)
		}
	}
}

func completeProducerMessage(message *sarama.ProducerMessage, err error) {
	if message == nil {
		return
	}

	receipt, ok := message.Metadata.(*producerReceipt)
	if !ok || receipt == nil {
		return
	}

	receipt.attempt.resolve(receipt.index, err)
}

func missingTopicIndexes(results []error) []int {
	indexes := make([]int, 0, len(results))
	for i, err := range results {
		if errors.Is(err, sarama.ErrUnknownTopicOrPartition) {
			indexes = append(indexes, i)
		}
	}

	return indexes
}

// toSaramaMessage converts a broker message into a Sarama producer message.
func toSaramaMessage(topic string, message BrokerMessage, metadata any) *sarama.ProducerMessage {
	headers := make([]sarama.RecordHeader, 0, len(message.Headers))
	for key, value := range message.Headers {
		headers = append(headers, sarama.RecordHeader{Key: []byte(key), Value: value})
	}

	return &sarama.ProducerMessage{
		Topic:    topic,
		Value:    sarama.ByteEncoder(message.Payload),
		Headers:  headers,
		Metadata: metadata,
	}
}

// Close flushes the asynchronous producer, waits for the result dispatcher,
// closes the topic manager, and is idempotent.
func (c *ControllerWriter) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return c.closeErr
	}
	c.closed = true
	c.active.Wait()
	c.closeTopicRetries()

	var closeErrors []error
	if c.producer == nil {
		close(c.done)
	} else {
		if err := c.producer.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close producer: %w", err))
		}
		<-c.done
	}

	if c.topicManager != nil {
		if err := c.topicManager.close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close topic manager: %w", err))
		}
	}
	c.closeErr = errors.Join(closeErrors...)

	return c.closeErr
}
