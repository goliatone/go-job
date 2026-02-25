package queue

import (
	"encoding/json"
	"fmt"

	job "github.com/goliatone/go-job"
)

type messageEnvelope struct {
	JobID           string                  `json:"job_id"`
	ScriptPath      string                  `json:"script_path"`
	MachineID       string                  `json:"machine_id,omitempty"`
	EntityID        string                  `json:"entity_id,omitempty"`
	ExecutionID     string                  `json:"execution_id,omitempty"`
	ExpectedState   string                  `json:"expected_state,omitempty"`
	ExpectedVersion int64                   `json:"expected_version,omitempty"`
	ResumeEvent     string                  `json:"resume_event,omitempty"`
	Config          job.Config              `json:"config,omitempty"`
	Parameters      map[string]any          `json:"parameters,omitempty"`
	IdempotencyKey  string                  `json:"idempotency_key,omitempty"`
	DedupPolicy     job.DeduplicationPolicy `json:"dedup_policy,omitempty"`
	Result          *job.Result             `json:"result,omitempty"`
}

// EncodeExecutionMessage marshals a message while preserving raw payload bytes.
func EncodeExecutionMessage(msg *job.ExecutionMessage) ([]byte, error) {
	if msg == nil {
		return nil, fmt.Errorf("execution message required")
	}

	var params map[string]any
	if len(msg.Parameters) > 0 {
		params = cloneParams(msg.Parameters)
		if payload, ok := params["payload"]; ok {
			switch value := payload.(type) {
			case []byte:
				params["payload"] = json.RawMessage(cloneBytes(value))
			case json.RawMessage:
				params["payload"] = json.RawMessage(cloneBytes(value))
			}
		}
	}

	envelope := messageEnvelope{
		JobID:           msg.JobID,
		ScriptPath:      msg.ScriptPath,
		MachineID:       msg.MachineID,
		EntityID:        msg.EntityID,
		ExecutionID:     msg.ExecutionID,
		ExpectedState:   msg.ExpectedState,
		ExpectedVersion: msg.ExpectedVersion,
		ResumeEvent:     msg.ResumeEvent,
		Config:          msg.Config,
		Parameters:      params,
		IdempotencyKey:  msg.IdempotencyKey,
		DedupPolicy:     msg.DedupPolicy,
		Result:          msg.Result,
	}

	return json.Marshal(envelope)
}

type messageEnvelopeRaw struct {
	JobID           string                     `json:"job_id"`
	ScriptPath      string                     `json:"script_path"`
	MachineID       string                     `json:"machine_id,omitempty"`
	EntityID        string                     `json:"entity_id,omitempty"`
	ExecutionID     string                     `json:"execution_id,omitempty"`
	ExpectedState   string                     `json:"expected_state,omitempty"`
	ExpectedVersion int64                      `json:"expected_version,omitempty"`
	ResumeEvent     string                     `json:"resume_event,omitempty"`
	Config          job.Config                 `json:"config,omitempty"`
	Parameters      map[string]json.RawMessage `json:"parameters,omitempty"`
	IdempotencyKey  string                     `json:"idempotency_key,omitempty"`
	DedupPolicy     job.DeduplicationPolicy    `json:"dedup_policy,omitempty"`
	Result          *job.Result                `json:"result,omitempty"`
}

// DecodeExecutionMessage unmarshals a message and restores raw payload bytes.
func DecodeExecutionMessage(payload []byte) (*job.ExecutionMessage, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("payload empty")
	}

	var raw messageEnvelopeRaw
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, err
	}

	msg := &job.ExecutionMessage{
		JobID:           raw.JobID,
		ScriptPath:      raw.ScriptPath,
		MachineID:       raw.MachineID,
		EntityID:        raw.EntityID,
		ExecutionID:     raw.ExecutionID,
		ExpectedState:   raw.ExpectedState,
		ExpectedVersion: raw.ExpectedVersion,
		ResumeEvent:     raw.ResumeEvent,
		Config:          raw.Config,
		IdempotencyKey:  raw.IdempotencyKey,
		DedupPolicy:     raw.DedupPolicy,
		Result:          raw.Result,
	}

	if len(raw.Parameters) > 0 {
		params := make(map[string]any, len(raw.Parameters))
		for key, value := range raw.Parameters {
			if key == "payload" {
				if value != nil {
					params[key] = cloneBytes(value)
				}
				continue
			}
			if len(value) == 0 {
				params[key] = nil
				continue
			}
			var decoded any
			if err := json.Unmarshal(value, &decoded); err != nil {
				return nil, err
			}
			params[key] = decoded
		}
		msg.Parameters = params
	}

	return msg, nil
}

func cloneParams(params map[string]any) map[string]any {
	out := make(map[string]any, len(params))
	for key, value := range params {
		out[key] = value
	}
	return out
}

func cloneBytes(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	out := make([]byte, len(value))
	copy(out, value)
	return out
}
