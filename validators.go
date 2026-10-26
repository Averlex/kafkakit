package kafka

import (
	"fmt"

	"github.com/IBM/sarama"
)

// validate checks all options and returns an errors if any of them are present.
func validate(cfg Config) error {
	var errors []fieldError

	if cfg.DSN == "" {
		errors = append(errors, fieldError{
			Field:   "DSN",
			Message: "broker DSN is required",
		})
	}

	if cfg.Mode != ModeConsumer && cfg.Mode != ModeProducer && cfg.Mode != ModeMixed {
		errors = append(errors, fieldError{
			Field:   "Mode",
			Message: fmt.Sprintf("unknown mode: %s", cfg.Mode),
		})
	}

	if cfg.GroupID == "" && (cfg.Mode == ModeMixed || cfg.Mode == ModeConsumer) {
		errors = append(errors, fieldError{
			Field:   "GroupID",
			Message: "group id is required",
		})
	}

	if cfg.MaxBytes <= 0 {
		errors = append(errors, fieldError{
			Field:   "MaxBytes",
			Message: fmt.Sprintf("must be greater than 0: %v", cfg.MaxBytes),
		})
	} else if cfg.MaxBytes >= int(sarama.MaxRequestSize) {
		errors = append(errors, fieldError{
			Field: "MaxBytes",
			Message: fmt.Sprintf(
				"must be less than Sarama's maximum request size of %d bytes: %v",
				sarama.MaxRequestSize,
				cfg.MaxBytes,
			),
		})
	}

	if cfg.BatchBytes <= 0 {
		errors = append(errors, fieldError{
			Field:   "BatchBytes",
			Message: fmt.Sprintf("must be greater than 0: %v", cfg.BatchBytes),
		})
	} else if cfg.BatchBytes >= int64(sarama.MaxRequestSize) {
		errors = append(errors, fieldError{
			Field: "BatchBytes",
			Message: fmt.Sprintf(
				"must be less than Sarama's maximum request size of %d bytes: %v",
				sarama.MaxRequestSize,
				cfg.BatchBytes,
			),
		})
	}

	if cfg.TopicCreateAttempts < 0 {
		errors = append(errors, fieldError{
			Field:   "TopicCreateAttempts",
			Message: fmt.Sprintf("must be greater than or equal to 0: %v", cfg.TopicCreateAttempts),
		})
	}

	if cfg.NackBackoff <= 0 {
		errors = append(errors, fieldError{
			Field:   "NackBackoff",
			Message: fmt.Sprintf("must be greater than 0: %v", cfg.NackBackoff),
		})
	}

	if len(errors) > 0 {
		return &validationError{Fields: errors}
	}

	return nil
}
