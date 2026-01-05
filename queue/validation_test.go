package queue

import (
	"testing"

	job "github.com/goliatone/go-job"
)

func TestValidateRequiredMessage(t *testing.T) {
	tests := []struct {
		name    string
		msg     *job.ExecutionMessage
		wantErr bool
	}{
		{
			name:    "nil message",
			msg:     nil,
			wantErr: true,
		},
		{
			name:    "missing job id",
			msg:     &job.ExecutionMessage{ScriptPath: "script.sh"},
			wantErr: true,
		},
		{
			name:    "missing script path",
			msg:     &job.ExecutionMessage{JobID: "job-1"},
			wantErr: true,
		},
		{
			name:    "valid message",
			msg:     &job.ExecutionMessage{JobID: "job-1", ScriptPath: "script.sh"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRequiredMessage(tt.msg)
			if tt.wantErr && err == nil {
				t.Fatalf("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
