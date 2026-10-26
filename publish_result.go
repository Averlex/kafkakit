package kafka

import "sync"

// PublishResult reports completion of one asynchronous publication.
// Err must only be inspected after Done is closed.
type PublishResult interface {
	Done() <-chan struct{}
	Err() error
}

// BatchPublishResult reports completion of an asynchronous batch publication.
// MessageErrors must only be inspected after Done is closed. It returns one
// error per input message when individual outcomes are available, or nil when
// the operation failed as a whole.
type BatchPublishResult interface {
	PublishResult
	MessageErrors() []error
}

type publishResult struct {
	done chan struct{}

	mu            sync.RWMutex
	err           error
	messageErrors []error
	completeOnce  sync.Once
}

func newPendingPublishResult() *publishResult {
	return &publishResult{done: make(chan struct{})}
}

func newCompletedPublishResult(err error, messageErrors []error) *publishResult {
	result := newPendingPublishResult()
	result.complete(err, messageErrors)

	return result
}

// Done returns a channel that is closed when publication completes.
func (r *publishResult) Done() <-chan struct{} {
	return r.done
}

// Err returns the overall publication error after Done is closed.
func (r *publishResult) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.err
}

// MessageErrors returns a copy of the per-message batch outcomes after Done is
// closed. A nil element means that the corresponding message was published.
func (r *publishResult) MessageErrors() []error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return cloneErrors(r.messageErrors)
}

func (r *publishResult) complete(err error, messageErrors []error) {
	r.completeOnce.Do(func() {
		r.mu.Lock()
		r.err = err
		r.messageErrors = cloneErrors(messageErrors)
		close(r.done)
		r.mu.Unlock()
	})
}

func cloneErrors(errors []error) []error {
	if errors == nil {
		return nil
	}

	result := make([]error, len(errors))
	copy(result, errors)

	return result
}
