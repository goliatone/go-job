package queue

import (
	"github.com/goliatone/go-errors"
	job "github.com/goliatone/go-job"
)

// ValidateRequiredMessage enforces required ExecutionMessage fields.
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
	if msg.ScriptPath == "" {
		fieldErrors = append(fieldErrors, errors.FieldError{
			Field:   "script_path",
			Message: "required",
			Value:   msg.ScriptPath,
		})
	}

	if len(fieldErrors) > 0 {
		return errors.NewValidation("execution message validation failed", fieldErrors...)
	}

	return nil
}
