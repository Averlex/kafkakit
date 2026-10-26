package kafka_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/Averlex/kafkakit"
)

// region Suite

type ErrorSuite struct {
	suite.Suite
}

func TestErrorSuite(t *testing.T) {
	suite.Run(t, new(ErrorSuite))
}

// endregion

// region Tests

var errTest = errors.New("test error")

func (s *ErrorSuite) TestPublishError_UsableWhenBuiltByHand() {

	// Callers build this themselves - in tests, or when wrapping a batch result -
	// so a value with only Errs set must behave like any other error.
	err := &kafka.PublishError{Errs: []error{nil, errTest, nil}}

	s.Require().Contains(err.Error(), errTest.Error(), "message must name the failure")

	results, ok := kafka.BatchErrors(err)
	s.Require().True(ok, "per-message errors must be readable")
	s.Require().Equal([]error{nil, errTest, nil}, results, "results stay aligned with the batch")
}

func (s *ErrorSuite) TestPublishError_CauseIsReachable() {
	// The point of unwrapping: the caller classifies the cause without knowing
	// whether the failure was partial or total.
	err := &kafka.PublishError{Errs: []error{nil, context.DeadlineExceeded}}

	s.Require().ErrorIs(err, context.DeadlineExceeded, "errors.Is must reach a per-message cause")
}

func (s *ErrorSuite) TestPublishError_NoErrors() {
	// A batch with nothing wrong in it is degenerate, but must not panic.
	err := &kafka.PublishError{Errs: []error{nil, nil}}

	s.Require().NotPanics(func() { _ = err.Error() }, "Error must not panic without failures")
	s.Require().NotErrorIs(err, errTest, "there is no cause to find")
}

func (s *ErrorSuite) TestBatchErrors_NotABatchFailure() {
	testCases := []struct {
		err  error
		name string
	}{
		{name: "nil", err: nil},
		{name: "plain error", err: errTest},
		{name: "wrapped plain error", err: errors.Join(errTest)},
	}

	for _, tC := range testCases {
		s.Run(tC.name, func() {
			results, ok := kafka.BatchErrors(tC.err)
			s.Require().False(ok, "only a partial batch failure carries per-message errors")
			s.Require().Nil(results, "no results to report")
		})
	}
}

// endregion
