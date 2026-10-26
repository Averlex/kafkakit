package kafka

import (
	"errors"
	"fmt"
	"strings"
)

// ErrWriterClosed is returned by Publish when the producer has been closed.
var ErrWriterClosed = errors.New("kafka writer closed")

// PublishError reports a batch publish in which some messages were written and
// some were not. A batch that failed as a whole is reported as a plain error
// instead, since there is nothing to tell apart.
type PublishError struct {
	Errs []error
}

// newPublishError builds an error for the given per-message results, or returns
// nil when every message was published.
func newPublishError(errs []error) error {
	for _, err := range errs {
		if err != nil {
			return &PublishError{Errs: errs}
		}
	}

	return nil
}

// Error implements error interface.
func (e *PublishError) Error() string {
	failed := e.Unwrap()
	if len(failed) == 0 {
		return fmt.Sprintf("publish %d messages: no error", len(e.Errs))
	}

	return fmt.Sprintf("publish %d of %d messages: %v", len(failed), len(e.Errs), failed[0])
}

// Unwrap exposes the per-message errors to errors.Is and errors.As.
func (e *PublishError) Unwrap() []error {
	failed := make([]error, 0, len(e.Errs))
	for _, err := range e.Errs {
		if err != nil {
			failed = append(failed, err)
		}
	}

	return failed
}

// BatchErrors returns the per-message errors of a batch publish, aligned with
// the batch that was passed to PublishBatch.
func BatchErrors(err error) ([]error, bool) {
	var publishErr *PublishError
	if errors.As(err, &publishErr) {
		return publishErr.Errs, true
	}

	return nil, false
}

// validationError contains all validation errors.
type validationError struct {
	Fields []fieldError
}

// fieldError describes the validation error for a given field.
type fieldError struct {
	Field   string
	Message string
}

// Error implements error interface.
func (e *validationError) Error() string {
	if len(e.Fields) == 0 {
		return "validation error"
	}

	var sb strings.Builder
	sb.WriteString("validation failed:")
	for _, fe := range e.Fields {
		sb.WriteString(fmt.Sprintf(" %s- %s", fe.Field, fe.Message))
	}
	return sb.String()
}
