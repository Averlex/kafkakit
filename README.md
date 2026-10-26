# kafkakit

[![Go version](https://img.shields.io/badge/go-1.27.3+-blue.svg)](https://golang.org)
[![Go Reference](https://pkg.go.dev/badge/github.com/Averlex/kafkakit.svg)](https://pkg.go.dev/github.com/Averlex/kafkakit)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A configurable Kafka toolkit for Go with synchronous and asynchronous publishing, batch support, and consumer subscriptions with message redelivery.

## Contents

- [Client initialization](#client-initialization)
- [Configuration](#configuration)
- [Message encoding](#message-encoding)
- [Producer](#producer)
  - [Batch publishing](#batch-publishing)
  - [Asynchronous publishing](#asynchronous-publishing)
  - [Error handling](#error-handling)
- [Consumer](#consumer)
  - [Handler example](#handler-example)
- [Shutdown](#shutdown)
- [Development](#development)

## Client initialization

- `Connect()` and `Close()` are idempotent
- `Unsubscribe()` lets you unsubscribe from a specific topic without unsubscribing from the others

```go
ctx, cancel := context.WithCancel(context.Background())
cfg := kafka.DefaultConfig()
cfg.DSN = "<address>:<port>"
cfg.GroupID = "test_group_id"
b, err := kafka.NewBroker(&cfg)
if err != nil {
    log.Fatalf("failed to initialize Kafka client: %s", err.Error())
}

err = b.Connect(ctx)
if err != nil {
    log.Fatalf("failed to connect Kafka client: %s", err.Error())
}
defer b.Close(ctx)
```

## Configuration

Start with `kafka.DefaultConfig()`, then override the values you need. The defaults
in the tables below refer to this function. `NewBroker` does not apply defaults
to every zero-valued field: an empty `Mode` or zero `MaxBytes` fails validation,
and `ConnectionTest: false` disables connection checking.

Configuration fields have `json`, `yaml`, and `env` tags for use with a decoder
or environment loader of your choice. Load settings into the initialized config
to preserve defaults for omitted fields. The library does not load configuration
files or environment variables itself.

| Field | yaml / env | Default | Description |
|---|---|---|---|
| `DSN` | `dsn` / `KAFKA_DSN` | — | Broker address. Required |
| `Hosts` | `hosts` / `KAFKA_HOSTS` | from `DSN` | Comma-separated list of brokers. Falls back to `DSN` when empty |
| `GroupID` | `group_id` / `KAFKA_GROUP_ID` | — | Consumer group. Required for subscriptions |
| `Mode` | `mode` / `KAFKA_MODE` | `mixed` | `producer`, `consumer`, or `mixed`. Only affects which fields are required |
| `MaxBytes` | `max_bytes` / `KAFKA_MAX_BYTES` | 10 MB | Maximum message size when reading |
| `AutoCommit` | `auto_commit` / `KAFKA_AUTO_COMMIT` | `false` | Automatically commit offsets |
| `NackBackoff` | `nack_backoff` / `KAFKA_NACK_BACKOFF` | `1s` | Delay before redelivering a message after a handler error |
| `ConnectionTest` | `connection_test` / `KAFKA_CONNECTION_TEST` | `true` | Check the connection during `Connect()` |

Producer settings:

| Field | yaml / env | Default | Description |
|---|---|---|---|
| `BatchSize` | `batch_size` / `KAFKA_BATCH_SIZE` | 100 | Number of messages to accumulate before sending a batch |
| `BatchBytes` | `batch_bytes` / `KAFKA_BATCH_BYTES` | 1 MB | Maximum batch size in bytes |
| `BatchTimeout` | `batch_timeout` / `KAFKA_BATCH_TIMEOUT` | `10ms` | How long to wait before sending an incomplete batch |
| `TopicCreateAttempts` | `topic_create_attempts` / `KAFKA_TOPIC_CREATE_ATTEMPTS` | 0 | Number of attempts to create a missing topic; automatic creation is disabled by default |
| `TopicCreateBackoff` | `topic_create_backoff` / `KAFKA_TOPIC_CREATE_BACKOFF` | `100ms` | Delay between attempts while waiting for the topic to appear in metadata |

The client uses a single long-lived asynchronous Sarama producer. Messages from
concurrent `Publish` and `PublishBatch` calls may be combined into one network
batch. An incomplete batch is sent after `BatchTimeout`; reaching `BatchSize`
or `BatchBytes` triggers an earlier send. Synchronous methods return only after
Sarama reports success or failure for every submitted message.

## Message encoding

`Publish` accepts any JSON-serializable value. Structs, maps, slices, strings,
numbers, and booleans are encoded directly into the Kafka message body.
A `[]byte` value is sent unchanged. `Publish` always treats its argument as a
single message, even when the value is a slice. These messages are published
without Kafka headers.

A message envelope struct lets you provide Kafka headers. If the struct has
exported `Headers` and `Payload` fields, and `Headers` is a struct, `Payload`
is JSON-encoded as the message body, while fields in `Headers` with JSON tags
become Kafka headers:

```go
type Headers struct {
    MessageID string `json:"message_id"`
}
type Payload struct {
    Field1 string `json:"field1"`
    Field2 []string `json:"field2"`
    Field3 int64  `json:"field3"`
}
type Message struct {
    Headers Headers
    Payload Payload
}
```

The `FromBrokerMessage` function passed to each subscription handler performs
the reverse conversion. The destination can be a pointer to any type that can
be decoded from JSON, or a pointer to `[]byte` to retrieve the raw body.
For message envelopes, Kafka headers and the body are decoded separately.

## Producer

- With `TopicCreateAttempts == 0`, the library does not create missing topics. A positive value enables a limited number of creation attempts before publishing. This setting applies only to the producer; it is unused in `consumer` mode and affects only publishing in `mixed` mode
- `Publish()` blocks until the message is sent to the broker

```go
err = b.Publish(ctx, "topic-name", &exampleMessage)
if err != nil {
    log.Printf("failed to publish message to Kafka: %s", err.Error())
    return
}
log.Printf("message with ID %s sent successfully", id)
```

### Batch publishing

`PublishBatch()` accepts a slice and submits its elements to a single long-lived asynchronous producer. Sarama builds the actual network batches according to `BatchSize`, `BatchBytes`, and `BatchTimeout`. `Publish()` always treats its argument as a single message.

```go
msgs := []any{&firstMessage, &secondMessage, &thirdMessage}

err = b.PublishBatch(ctx, "topic-name", msgs)
```

`PublishBatch()` accepts any typed slice. Results follow the order of elements in the input slice.

### Asynchronous publishing

`PublishAsync` and `PublishBatchAsync` submit messages to the same long-lived
producer as their synchronous counterparts, but return before Kafka reports
the final delivery result. Encoding and enqueueing happen before the method
returns, so a call may temporarily block if the producer's input queue is full.

```go
result := b.PublishAsync(ctx, "topic-name", &message)

// Other work can run here while Kafka processes the message.

<-result.Done()
if err := result.Err(); err != nil {
    return err
}
```

Access `Err` and `MessageErrors` only after `Done` is closed.
`PublishBatchAsync` attempts to send every element that was successfully encoded,
even if another element failed. Individual message results always follow the
order of elements in the input slice:

```go
result := b.PublishBatchAsync(ctx, "topic-name", messages)
<-result.Done()

if err := result.Err(); err != nil {
    messageErrors := result.MessageErrors()
    if messageErrors == nil {
        // The operation failed as a whole; individual results are unknown.
        return err
    }

    for index, messageErr := range messageErrors {
        if messageErr != nil {
            // messages[index] was not sent. nil indicates a successful send.
        }
    }
}
```

`Err` is `nil` only when publishing succeeds completely. For a partial batch
failure, it returns `PublishError`, and `MessageErrors` contains one entry for
each input message. `MessageErrors` returns `nil` if the operation ends before
individual results are available, for example because producer initialization
failed or the batch argument was invalid.

When the context is canceled, all pending results complete with the context
error. A message already submitted to the producer may still reach Kafka,
so its delivery status should be considered unknown after context cancellation.

`Publish` and `PublishBatch` use the same asynchronous implementation internally,
but wait for it to complete before returning.

### Error handling

Publishing errors take two forms. Distinguishing between them matters because they require different responses.

**The entire call failed** — a regular error is returned, and nothing was sent to the broker. Examples include an unavailable cluster or a failed topic creation request.

**Part of the batch failed** — a `*PublishError` is returned with a result for each message. The remaining messages have been published. Use `BatchErrors` to inspect the results:

```go
if err := b.PublishBatch(ctx, "topic-name", msgs); err != nil {
    results, ok := kafka.BatchErrors(err)
    if !ok {
        // The entire batch was not sent; retry the whole batch.
        return err
    }

    for i, res := range results {
        if res != nil {
            log.Printf("message %d was not published: %s", i, res)
        }
        // res == nil means message msgs[i] was published.
    }
}
```

The `results` slice is aligned with the input: `results[i]` corresponds to `msgs[i]`. Messages that failed conversion are included in the same results and do not prevent the remaining messages from being sent.

You can also identify the cause without inspecting the slice: `errors.Is` and `errors.As` work with both error forms:

```go
if errors.Is(err, context.DeadlineExceeded) {
    // ...
}
```

## Consumer

`Subscribe()` subscribes the client to a topic and invokes the handler for each message.

- `Subscribe()` never creates a missing topic
- Returning `nil` from the handler acknowledges the message and allows the partition to advance
- Returning an error from the handler rejects the message. The same message is redelivered after `NackBackoff`, and its partition does not advance until processing succeeds
- The library does not impose a retry limit. After terminal handling or exhausting its own retry limit, the service must return `nil` to skip the message
- Format and decoding errors are usually terminal: log them and return `nil`. Return transient errors, such as database unavailability, from the handler to trigger redelivery

```go
done, err := b.Subscribe(ctx, "topic-name", handler)
if err != nil {
    log.Printf("failed to subscribe to Kafka: %s", err.Error())
}
```

### Handler example

- `FromBrokerMessage` is passed as the handler's last argument and can decode a message into any supported destination type
- The error returned by the handler controls negative acknowledgment (`nack`); it is not sent through `done`

```go
handler := func(_ context.Context, msg any, unmarshal func(any, any) error) error {
    var serializedMessage Message
    err := unmarshal(msg, &serializedMessage)
    if err != nil {
        log.Printf("skipping invalid message: %s", err.Error())
        return nil
    }
    log.Printf("message %s received successfully: %v", serializedMessage.Headers.MessageID, serializedMessage.Payload)
    // Process the message here. Return transient errors to trigger redelivery.
    return nil
}
```

## Shutdown

The `done` channel returned by `Subscribe()` signals that processing has finished for **one topic**.

If you subscribe to multiple topics, wait for **each** of their `done` channels to complete.

The primary way to shut down is to cancel the context:
```go
cancel()
<-done
```

`Close()` stops **all subscriptions** and closes the producer, flushing pending batches and releasing connections:
```go
b.Close(ctx)
<-done
```

`Unsubscribe()` stops **one subscription** by topic name:
```go
err := b.Unsubscribe(ctx, "topic-name")
if err != nil {
    // An error is returned if the subscription did not exist.
}
<-done // The channel returned by Subscribe for "topic-name".
```

For multiple topics:
```go
var wg sync.WaitGroup
for _, ch := range doneChannels {
    wg.Add(1)
    go func(c <-chan struct{}) {
        defer wg.Done()
        <-c
    }(ch)
}
cancel() // Or b.Close(ctx).
wg.Wait()
```
## Development

Use the Go version specified in `go.mod` and Make. To run tests, install
Mockery v3 separately and make the `mockery` executable available on `PATH`.
Mockery is not managed as a Go module tool dependency.

Generated mocks are not committed to the repository. After cloning, generate
them before running tests locally or in CI:

```sh
make gen
make test
```

Run `make gen` again after changing any mocked interface. It uses `.mockery.yml`
to generate `mocks/mocks.gen.go` for `Controller`, `Subscription`, and
`ControllerFactory`. Neither test target generates mocks automatically.

| Target | Description |
|---|---|
| `make gen` | Generate test mocks using the separately installed Mockery v3 |
| `make test` | Run all tests with verbose output (`go test -v ./...`) |
| `make test-coverage` | Run all tests with coverage and generate `coverage.out` and `coverage.html` |

To generate a coverage report after generating mocks:

```sh
make test-coverage
```

Open `coverage.html` in a browser to inspect coverage. gen files are local
artifacts excluded by `.gitignore`; do not commit them or the generated mocks.
Keep `.mockery.yml` and `.golangci.yml` in version control so contributors and
CI share the same generation and lint settings.

Applications can import and use the library without generating mocks or
installing Mockery; mocks are only required to run this repository's tests.
