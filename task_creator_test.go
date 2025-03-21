package job_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/goliatone/go-command"
	"github.com/goliatone/go-job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockEngine struct {
	mock.Mock
}

func (m *MockEngine) Name() string {
	return "mock_engine"
}

func (m *MockEngine) Execute(ctx context.Context, msg *job.ExecutionMessage) error {
	args := m.Called(ctx, msg)
	return args.Error(0)
}

func (m *MockEngine) CanHandle(path string) bool {
	args := m.Called(path)
	return args.Bool(0)
}

func (m *MockEngine) ParseJob(path string, content []byte) (job.Task, error) {
	args := m.Called(path, content)
	return args.Get(0).(job.Task), args.Error(1)
}

type MockSourceProvider struct {
	mock.Mock
}

func (m *MockSourceProvider) GetScript(path string) ([]byte, error) {
	args := m.Called(path)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockSourceProvider) ListScripts(ctx context.Context) ([]job.ScriptInfo, error) {
	args := m.Called(ctx)
	return args.Get(0).([]job.ScriptInfo), args.Error(1)
}

type MockTask struct {
	mock.Mock
}

func (m *MockTask) GetID() string {
	args := m.Called()
	return args.Get(0).(string)
}

func (m *MockTask) GetHandler() func() error {
	return func() error {
		return nil
	}
}

func (m *MockTask) GetHandlerConfig() command.HandlerConfig {
	args := m.Called()
	return args.Get(0).(command.HandlerConfig)
}

func (m *MockTask) GetConfig() job.Config {
	args := m.Called()
	return args.Get(0).(job.Config)
}

func TestTaskCreator_WithErrorHandler(t *testing.T) {
	mockProvider := new(MockSourceProvider)
	mockEngine := new(MockEngine)
	engines := []job.Engine{mockEngine}

	expectedErr := errors.New("test error")
	var capturedErr error
	customErrorHandler := func(err error) {
		assert.NotNil(t, err)
		capturedErr = err
	}

	// test that customErrorHandler is called when we have a ParseJob error
	mockProvider.On("ListScripts", mock.Anything).Return([]job.ScriptInfo{
		{
			ID:   "1",
			Path: "testdata/example.js",
		},
	}, nil)

	mockEngine.On("CanHandle", mock.Anything, mock.Anything).Return(true)
	mockEngine.On("ParseJob", mock.Anything, mock.Anything).Return(&MockTask{}, expectedErr)

	creator := job.NewTaskCreator(mockProvider, engines)

	assert.NotNil(t, creator, "TaskCreator should not be nil")

	result := creator.WithErrorHandler(customErrorHandler)
	assert.Same(t, creator, result, "WithErrorHandler should return the same instance for chaining")

	r, e := creator.CreateTasks(context.Background())
	assert.Len(t, r, 0)
	assert.Nil(t, e)
	assert.Error(t, capturedErr)
	assert.Contains(t, capturedErr.Error(), expectedErr.Error())
}

func TestCreateTasksWithDeadline(t *testing.T) {
	mockProvider := new(MockSourceProvider)
	mockEngine := new(MockEngine)
	engines := []job.Engine{mockEngine}

	mockProvider.On("ListScripts", mock.Anything).Return([]job.ScriptInfo{
		{ID: "1", Path: "testdata/example.js"},
	}, nil)

	mockEngine.On("CanHandle", "testdata/example.js").Return(true)
	mockEngine.On("ParseJob", "testdata/example.js", mock.Anything).Return(&MockTask{}, nil)

	creator := job.NewTaskCreator(mockProvider, engines)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
	defer cancel()

	tasks, err := creator.CreateTasks(ctx)
	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
}

func TestTaskCreator_CreateTasks(t *testing.T) {
	mockProvider := new(MockSourceProvider)
	mockEngine := new(MockEngine)
	engines := []job.Engine{mockEngine}

	mockProvider.On("ListScripts", mock.Anything).Return([]job.ScriptInfo{
		{ID: "1", Path: "testdata/example.js"},
	}, nil)

	mockEngine.On("CanHandle", "testdata/example.js").Return(true)
	mockEngine.On("ParseJob", "testdata/example.js", mock.Anything).Return(&MockTask{}, nil)

	creator := job.NewTaskCreator(mockProvider, engines)
	tasks, err := creator.CreateTasks(context.Background())

	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
}
