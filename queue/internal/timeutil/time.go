package timeutil

import "time"

// UnixNanoTime converts a unix nanoseconds timestamp to UTC time.
func UnixNanoTime(ts int64) time.Time {
	if ts <= 0 {
		return time.Time{}
	}
	return time.Unix(0, ts).UTC()
}

// PtrTime returns a pointer to UTC time for non-zero values.
func PtrTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	v := value.UTC()
	return &v
}
