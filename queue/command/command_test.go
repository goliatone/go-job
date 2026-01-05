package command

import (
	"context"
	"testing"

	"github.com/goliatone/go-command"
	job "github.com/goliatone/go-job"
)

type testMessage struct {
	Name string `json:"name"`
}

type testCommand struct {
	got string
}

func (c *testCommand) Execute(_ context.Context, msg testMessage) error {
	c.got = msg.Name
	return nil
}

type pointerCommand struct {
	got string
}

func (c *pointerCommand) Execute(_ context.Context, msg *testMessage) error {
	if msg != nil {
		c.got = msg.Name
	}
	return nil
}

func TestTaskExecutesRegisteredCommand(t *testing.T) {
	reg := NewRegistry()
	cmd := &testCommand{}

	if err := RegisterCommand(reg, cmd); err != nil {
		t.Fatalf("register: %v", err)
	}

	id := command.GetMessageType(testMessage{})
	task := NewTask(reg, id)

	err := task.Execute(context.Background(), &job.ExecutionMessage{
		Parameters: map[string]any{"name": "alpha"},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if cmd.got != "alpha" {
		t.Fatalf("expected command to receive params, got %q", cmd.got)
	}
}

func TestTaskExecutesPointerCommand(t *testing.T) {
	reg := NewRegistry()
	cmd := &pointerCommand{}

	if err := RegisterCommand(reg, cmd); err != nil {
		t.Fatalf("register: %v", err)
	}

	id := command.GetMessageType(&testMessage{})
	task := NewTask(reg, id)

	err := task.Execute(context.Background(), &job.ExecutionMessage{
		Parameters: map[string]any{"name": "beta"},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if cmd.got != "beta" {
		t.Fatalf("expected command to receive params, got %q", cmd.got)
	}
}
