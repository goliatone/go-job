package job

var defaultIdempotencyTracker = NewIdempotencyTracker()

func dedupBeforeExecute(tracker *IdempotencyTracker, msg *ExecutionMessage) (dedupDecision, error) {
	if tracker == nil || msg == nil {
		return dedupProceed, nil
	}
	return tracker.BeforeExecute(msg.IdempotencyKey, msg.DedupPolicy)
}

func dedupAfterExecute(tracker *IdempotencyTracker, msg *ExecutionMessage, execErr *error) {
	if tracker == nil || msg == nil {
		return
	}
	var err error
	if execErr != nil {
		err = *execErr
	}
	tracker.AfterExecute(msg.IdempotencyKey, msg.DedupPolicy, err)
}
