package job_test

import (
	"testing"

	"github.com/goliatone/go-job"
	"github.com/stretchr/testify/assert"
)

func TestYAMLMetadataParser_Parse_Shell(t *testing.T) {
	parser := job.NewYAMLMetadataParser()
	content := []byte(`
# config
# schedule: "*/5 * * * *"
# timeout: 120
echo "Hello, world!"`)

	config, script, err := parser.Parse(content)

	assert.NoError(t, err)
	assert.Equal(t, "*/5 * * * *", config.Schedule)
	// 120 interpreted as 120 seconds
	assert.Equal(t, 120, int(config.Timeout.Seconds()))
	assert.Equal(t, "echo \"Hello, world!\"", script)
}

func TestYAMLMetadataParser_Parse_JS(t *testing.T) {
	parser := job.NewYAMLMetadataParser()
	content := []byte(`
// config
// schedule: "0 12 * * *"
// timeout: 300s

console.log("Running script...");`)

	config, script, err := parser.Parse(content)

	assert.NoError(t, err)
	assert.Equal(t, "0 12 * * *", config.Schedule)
	// "300s" parsed correctly
	assert.Equal(t, 300, int(config.Timeout.Seconds()))
	assert.Equal(t, "\nconsole.log(\"Running script...\");", script)
}

func TestYAMLMetadataParser_Parse_NoMetadata(t *testing.T) {
	parser := job.NewYAMLMetadataParser()
	content := []byte("echo 'No metadata'")

	config, script, err := parser.Parse(content)

	assert.NoError(t, err)
	assert.Equal(t, job.DefaultSchedule, config.Schedule)
	assert.Equal(t, 60, int(config.Timeout.Seconds()))
	assert.Equal(t, "echo 'No metadata'", script)
}

func TestYAMLMetadataParser_Parse_InvalidTimeout(t *testing.T) {
	parser := job.NewYAMLMetadataParser()
	content := []byte(`---
schedule: "*/10 * * * *"
timeout: "notaduration"
---
echo "Broken timeout"`)

	config, script, err := parser.Parse(content)

	// We expect an error because the timeout string is not valid.
	assert.Error(t, err)
	// The default timeout should still be applied.
	assert.Equal(t, 60, int(config.Timeout.Seconds()))
	assert.Equal(t, "echo \"Broken timeout\"", script)
}

func TestYAMLMetadataParser_Parse_NumberWithUnderscores(t *testing.T) {
	parser := job.NewYAMLMetadataParser()
	content := []byte(`
# config
# schedule: "*/15 * * * *"
# retries: 30
# timeout: 30_000
# debug: true
# run_once: true
# script_type: shell
# transaction: true
# env:
#  APP_NAME: test
#  APP_PORT: 1234
# metadata:
#  test1: test
#  test2: 2
echo "Timeout with underscores"`)

	env := map[string]string{
		"APP_NAME": "test",
		"APP_PORT": "1234",
	}
	meta := map[string]any{
		"test1": "test",
		"test2": 2,
	}

	config, script, err := parser.Parse(content)

	assert.NoError(t, err)
	assert.Equal(t, "*/15 * * * *", config.Schedule)
	// "30_000" becomes 30000 seconds after cleaning.
	assert.Equal(t, 30000, int(config.Timeout.Seconds()))
	assert.Equal(t, 30, config.Retries)
	assert.Equal(t, true, config.Debug)
	assert.Equal(t, true, config.RunOnce)
	assert.Equal(t, true, config.Transaction)
	assert.Equal(t, "shell", config.ScriptType)
	assert.Equal(t, env, config.Env)
	assert.Equal(t, meta, config.Metadata)
	assert.Equal(t, "echo \"Timeout with underscores\"", script)
}

func TestYAMLMetadataParser_Parse_WithCruft(t *testing.T) {
	parser := job.NewYAMLMetadataParser()
	content := []byte(`
# config
# schedule: "*/15 * * * *"
# this_is: nothing!
# timeout: 30_000
echo "Timeout with underscores"`)

	config, script, err := parser.Parse(content)

	assert.NoError(t, err)
	assert.Equal(t, "*/15 * * * *", config.Schedule)
	// "30_000" becomes 30000 seconds after cleaning.
	assert.Equal(t, 30000, int(config.Timeout.Seconds()))
	assert.Equal(t, "echo \"Timeout with underscores\"", script)
}
