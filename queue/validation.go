package queue

import (
	"fmt"

	"github.com/goliatone/go-errors"
	job "github.com/goliatone/go-job"
)

// ValidateRequiredMessage enforces required ExecutionMessage fields for queue usage.
func ValidateRequiredMessage(msg *job.ExecutionMessage) error {
	if msg == nil {
		return errors.NewValidation("execution message required",
			errors.FieldError{
				Field:   "execution_message",
				Message: "cannot be nil",
			},
		)
	}

	var fieldErrors []errors.FieldError
	if msg.JobID == "" {
		fieldErrors = append(fieldErrors, errors.FieldError{
			Field:   "job_id",
			Message: "required",
			Value:   msg.JobID,
		})
	}

	if len(fieldErrors) > 0 {
		return errors.NewValidation("execution message validation failed", fieldErrors...)
	}

	return nil
}

// ValidateNackOptions enforces explicit nack disposition semantics.
func ValidateNackOptions(opts NackOptions) error {
	switch opts.Disposition {
	case NackDispositionRetry:
		if opts.Delay < 0 {
			return fmt.Errorf("retry delay must be >= 0")
		}
		return nil
	case NackDispositionDeadLetter, NackDispositionFailed, NackDispositionCanceled:
		return nil
	default:
		return fmt.Errorf("nack disposition required")
	}
}
