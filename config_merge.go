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
	if override.ScriptType != "" {
		result.ScriptType = override.ScriptType
	}
	if override.Transaction {
		result.Transaction = true
	}
	if override.Metadata != nil {
		result.Metadata = override.Metadata
	}
	if override.Env != nil {
		result.Env = override.Env
	}

	return result
}
