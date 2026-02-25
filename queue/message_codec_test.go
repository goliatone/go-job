package queue

import (
	"testing"

	job "github.com/goliatone/go-job"
	"github.com/stretchr/testify/require"
)

func TestExecutionMessageCodecRoundTripWithFSMFields(t *testing.T) {
	msg := &job.ExecutionMessage{
		JobID:           "job-1",
		ScriptPath:      "/tmp/job-1",
		MachineID:       "machine-a",
		EntityID:        "entity-42",
		ExecutionID:     "exec-123",
		ExpectedState:   "pending",
		ExpectedVersion: 7,
		ResumeEvent:     "resume.timeout",
		Parameters: map[string]any{
			"payload": []byte(`{"hello":"world"}`),
			"count":   2,
		},
		IdempotencyKey: "idem-1",
		DedupPolicy:    job.DedupPolicyDrop,
	}

	payload, err := EncodeExecutionMessage(msg)
	require.NoError(t, err)

	decoded, err := DecodeExecutionMessage(payload)
	require.NoError(t, err)
	require.NotNil(t, decoded)

	require.Equal(t, msg.JobID, decoded.JobID)
	require.Equal(t, msg.ScriptPath, decoded.ScriptPath)
	require.Equal(t, msg.MachineID, decoded.MachineID)
	require.Equal(t, msg.EntityID, decoded.EntityID)
	require.Equal(t, msg.ExecutionID, decoded.ExecutionID)
	require.Equal(t, msg.ExpectedState, decoded.ExpectedState)
	require.Equal(t, msg.ExpectedVersion, decoded.ExpectedVersion)
	require.Equal(t, msg.ResumeEvent, decoded.ResumeEvent)
	require.Equal(t, msg.IdempotencyKey, decoded.IdempotencyKey)
	require.Equal(t, msg.DedupPolicy, decoded.DedupPolicy)
	require.Equal(t, []byte(`{"hello":"world"}`), decoded.Parameters["payload"])
	require.EqualValues(t, 2, decoded.Parameters["count"])
}
