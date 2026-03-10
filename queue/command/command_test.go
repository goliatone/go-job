package command

import (
	"context"
	"reflect"
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

func TestRegisterCommandByTypeWithMetaValue(t *testing.T) {
	reg := NewRegistry()
	cmd := &testCommand{}
	meta := command.CommandMeta{
		MessageType:      command.GetMessageType(testMessage{}),
		MessageTypeValue: reflect.TypeOf(testMessage{}),
		MessageValue:     testMessage{},
	}

	if err := RegisterCommandByType(reg, cmd, meta); err != nil {
		t.Fatalf("register: %v", err)
	}

	task := NewTask(reg, meta.MessageType)
	err := task.Execute(context.Background(), &job.ExecutionMessage{
		Parameters: map[string]any{"name": "gamma"},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if cmd.got != "gamma" {
		t.Fatalf("expected command to receive params, got %q", cmd.got)
	}
}

func TestRegisterCommandByTypeWithMetaPointer(t *testing.T) {
	reg := NewRegistry()
	cmd := &pointerCommand{}
	meta := command.CommandMeta{
		MessageType:      command.GetMessageType(&testMessage{}),
		MessageTypeValue: reflect.TypeOf((*testMessage)(nil)),
		MessageValue:     &testMessage{},
	}

	if err := RegisterCommandByType(reg, cmd, meta); err != nil {
		t.Fatalf("register: %v", err)
	}

	task := NewTask(reg, meta.MessageType)
	err := task.Execute(context.Background(), &job.ExecutionMessage{
		Parameters: map[string]any{"name": "delta"},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if cmd.got != "delta" {
		t.Fatalf("expected command to receive params, got %q", cmd.got)
	}
}

func TestRegisterCommandByTypeFallbackType(t *testing.T) {
	reg := NewRegistry()
	cmd := &testCommand{}
	meta := command.CommandMeta{
		MessageType: command.GetMessageType(testMessage{}),
	}

	if err := RegisterCommandByType(reg, cmd, meta); err != nil {
		t.Fatalf("register: %v", err)
	}
	if len(reg.List()) != 1 {
		t.Fatalf("expected 1 command, got %d", len(reg.List()))
	}
}

func TestQueueResolverRegistersCommand(t *testing.T) {
	reg := NewRegistry()
	cmd := &testCommand{}
	meta := command.CommandMeta{
		MessageType:      command.GetMessageType(testMessage{}),
		MessageTypeValue: reflect.TypeOf(testMessage{}),
		MessageValue:     testMessage{},
	}

	if err := QueueResolver(reg)(cmd, meta, nil); err != nil {
		t.Fatalf("resolver: %v", err)
	}
	if len(reg.List()) != 1 {
		t.Fatalf("expected 1 command, got %d", len(reg.List()))
	}
}

func TestRegisterCommandByTypeDuplicateConflict(t *testing.T) {
	reg := NewRegistry()
	cmd := &testCommand{}
	meta := command.CommandMeta{
		MessageType:      command.GetMessageType(testMessage{}),
		MessageTypeValue: reflect.TypeOf(testMessage{}),
		MessageValue:     testMessage{},
	}

	if err := RegisterCommandByType(reg, cmd, meta); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := RegisterCommandByType(reg, cmd, meta); err == nil {
		t.Fatal("expected duplicate registration conflict")
	}
	if len(reg.List()) != 1 {
		t.Fatalf("expected 1 command, got %d", len(reg.List()))
	}
}

func TestQueueResolverDuplicateConflict(t *testing.T) {
	reg := NewRegistry()
	cmd := &testCommand{}
	meta := command.CommandMeta{
		MessageType:      command.GetMessageType(testMessage{}),
		MessageTypeValue: reflect.TypeOf(testMessage{}),
		MessageValue:     testMessage{},
	}

	resolver := QueueResolver(reg)
	if err := resolver(cmd, meta, nil); err != nil {
		t.Fatalf("resolver register: %v", err)
	}
	err := resolver(cmd, meta, nil)
	if err == nil {
		t.Fatal("expected duplicate registration conflict")
	}
}
