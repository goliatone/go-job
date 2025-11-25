package job_test

import (
	"testing"
	"time"

	"github.com/goliatone/go-errors"
	"github.com/goliatone/go-job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResultEncodeDecodeRoundTrip(t *testing.T) {
	res := job.Result{
		Status:    "success",
		Message:   "ok",
		OutputURL: "https://example.com/output",
		Size:      2048,
		Duration:  2 * time.Second,
		Metadata:  map[string]any{"format": "csv"},
	}

	payload, err := job.EncodeResult(res)
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	decoded, err := job.DecodeResult(payload)
	require.NoError(t, err)
	assert.Equal(t, res.Status, decoded.Status)
	assert.Equal(t, res.Message, decoded.Message)
	assert.Equal(t, res.OutputURL, decoded.OutputURL)
	assert.Equal(t, res.Size, decoded.Size)
	assert.Equal(t, res.Duration, decoded.Duration)
	assert.Equal(t, res.Metadata, decoded.Metadata)
}

func TestResultEncodeSizeLimit(t *testing.T) {
	res := job.Result{Message: "too big"}
	_, err := job.EncodeResult(res, job.WithResultMaxBytes(4))
	require.Error(t, err)
	fields, ok := errors.GetValidationErrors(err)
	require.True(t, ok)
	assert.Equal(t, "result", fields[0].Field)
}

func TestResultValidationNonNegative(t *testing.T) {
	_, err := job.EncodeResult(job.Result{Size: -1})
	require.Error(t, err)
	fields, ok := errors.GetValidationErrors(err)
	require.True(t, ok)
	assert.Equal(t, "size", fields[0].Field)
}

func TestRegistryStoresResult(t *testing.T) {
	reg := job.NewMemoryRegistry()
	task := &countingTask{id: "result-task", path: "/tmp/result"}
	require.NoError(t, reg.Add(task))

	result := job.Result{
		Status:   "success",
		Message:  "done",
		Size:     10,
		Duration: time.Second,
	}

	require.NoError(t, reg.SetResult(task.id, result))

	got, ok := reg.GetResult(task.id)
	require.True(t, ok)
	assert.Equal(t, result, got)
}
