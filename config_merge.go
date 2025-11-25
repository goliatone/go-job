package job

// mergeConfigDefaults overlays override onto base, preserving base values when
// overrides are left at zero values. Boolean fields are only promoted when the
// override is true to avoid accidentally clearing defaults.
func mergeConfigDefaults(base, override Config) Config {
	result := base

	if override.Schedule != "" {
		result.Schedule = override.Schedule
	}
	if override.Retries != 0 {
		result.Retries = override.Retries
	}
	if override.Timeout != 0 {
		result.Timeout = override.Timeout
	}
	if !override.Deadline.IsZero() {
		result.Deadline = override.Deadline
	}
	if override.NoTimeout {
		result.NoTimeout = true
	}
	if override.Debug {
		result.Debug = true
	}
	if override.RunOnce {
		result.RunOnce = true
	}
	if override.MaxRuns != 0 {
		result.MaxRuns = override.MaxRuns
	}
	if override.ExitOnError {
		result.ExitOnError = true
	}
	if override.MaxConcurrency != 0 {
		result.MaxConcurrency = override.MaxConcurrency
	}
	if override.ScriptType != "" {
		result.ScriptType = override.ScriptType
	}
	if override.Transaction {
		result.Transaction = true
	}
	if override.Backoff.Strategy != "" {
		result.Backoff.Strategy = override.Backoff.Strategy
	}
	if override.Backoff.Interval != 0 {
		result.Backoff.Interval = override.Backoff.Interval
	}
	if override.Backoff.MaxInterval != 0 {
		result.Backoff.MaxInterval = override.Backoff.MaxInterval
	}
	if override.Backoff.Jitter {
		result.Backoff.Jitter = true
	}
	if override.Metadata != nil {
		result.Metadata = override.Metadata
	}
	if override.Env != nil {
		result.Env = override.Env
	}
	if override.Backoff.Strategy != "" || override.Backoff.Interval != 0 || override.Backoff.MaxInterval != 0 || override.Backoff.Jitter {
		result.Backoff = mergeBackoffDefaults(base.Backoff, override.Backoff)
	}

	return result
}

func mergeBackoffDefaults(base, override BackoffConfig) BackoffConfig {
	result := base
	if override.Strategy != "" {
		result.Strategy = override.Strategy
	}
	if override.Interval != 0 {
		result.Interval = override.Interval
	}
	if override.MaxInterval != 0 {
		result.MaxInterval = override.MaxInterval
	}
	if override.Jitter {
		result.Jitter = true
	}
	return result
}
