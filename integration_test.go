package job_test

import (
	"context"
	"testing"

	"github.com/goliatone/go-command"
	gocron "github.com/goliatone/go-command/cron"
	"github.com/goliatone/go-job"
	"github.com/stretchr/testify/require"
)

func TestPayloadEnvelopeEncoding(t *testing.T) {
	env := job.Envelope{Params: map[string]any{"ping": "pong"}}
	encoded, err := job.EncodeEnvelope(env)
	require.NoError(t, err)
	decoded, err := job.DecodeEnvelope(encoded)
	require.NoError(t, err)
	require.Equal(t, env.Params["ping"], decoded.Params["ping"])
}

func TestIdempotencyDedupPolicies(t *testing.T) {
	task := &countingTask{id: "placeholder-dedup", path: "/tmp/dedup"}
	tracker := job.NewIdempotencyTracker()
	cmd := job.NewTaskCommander(task).WithIdempotencyTracker(tracker)
	msg := &job.ExecutionMessage{JobID: task.id, ScriptPath: task.path, IdempotencyKey: "dedup", DedupPolicy: job.DedupPolicyDrop}

	require.NoError(t, cmd.Execute(context.Background(), msg))
	require.ErrorIs(t, cmd.Execute(context.Background(), msg), job.ErrIdempotentDrop)
}

func TestResultMetadataPersistence(t *testing.T) {
	t.Skip("pending result metadata storage/retrieval with size caps and custom serializers")
}

func TestRetryBackoffProfiles(t *testing.T) {
	t.Skip("pending retry/backoff profiles (none/fixed/exponential with jitter) and defaults")
}

func TestConcurrencyQuotas(t *testing.T) {
	t.Skip("pending per-queue/per-job concurrency quotas and per-scope callbacks")
}

func TestCronManagementAPI(t *testing.T) {
	registry := job.NewMemoryRegistry()
	task := &countingTask{id: "cron-task", path: "/tmp/cron", cfg: job.Config{Schedule: "@hourly"}}
	require.NoError(t, registry.Add(task))

	scheduler := &integrationScheduler{}
	manager := job.NewCronManager(registry, scheduler)

	def := job.ScheduleDefinition{
		ID:         "cron-task",
		Expression: "*/5 * * * *",
		Message:    job.ExecutionMessage{JobID: task.id},
	}

	require.NoError(t, manager.Register(context.Background(), def))
	require.Equal(t, 1, scheduler.added)
	require.Equal(t, def.Expression, scheduler.lastCfg.Expression)
	require.Len(t, manager.List(), 1)

	result, err := manager.Reconcile(context.Background(), nil)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"cron-task"}, result.Removed)
	require.Empty(t, manager.List())
}

func TestObservabilityHooks(t *testing.T) {
	t.Skip("pending event/metric hooks for start/finish/fail/retry with correlation data")
}

func TestActorContextPropagation(t *testing.T) {
	t.Skip("pending optional go-auth adapter for actor/scope propagation and sanitization")
}

func TestStorageSchemaUpdates(t *testing.T) {
	t.Skip("pending storage interface/schema updates for idempotency, results, retry configs, cron schedules")
}

type integrationScheduler struct {
	added   int
	lastCfg command.HandlerConfig
}

func (s *integrationScheduler) AddHandler(cfg command.HandlerConfig, handler any) (gocron.Subscription, error) {
	s.added++
	s.lastCfg = cfg
	return integrationSubscription{}, nil
}

type integrationSubscription struct{}

func (integrationSubscription) Unsubscribe() {}
