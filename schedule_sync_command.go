package job

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/goliatone/go-command"
	"gopkg.in/yaml.v2"
)

// ScheduleLoader fetches desired schedules, e.g. from go-settings.
type ScheduleLoader func(ctx context.Context) ([]ScheduleDefinition, error)

// ScheduleSyncCommand reconciles schedules from an external source (settings) into the CronManager.
type ScheduleSyncCommand struct {
	manager *CronManager
	loader  ScheduleLoader

	cliName  string
	cliGroup string
	cliDesc  string
	cronExpr string
}

const defaultScheduleSyncCron = "*/5 * * * *"

// NewScheduleSyncCommand wires a sync command implementing both CLICommand and CronCommand.
func NewScheduleSyncCommand(manager *CronManager, loader ScheduleLoader, opts ...ScheduleSyncOption) *ScheduleSyncCommand {
	cmd := &ScheduleSyncCommand{
		manager:  manager,
		loader:   loader,
		cliName:  "sync-schedules",
		cliDesc:  "Reconcile cron schedules from settings",
		cronExpr: defaultScheduleSyncCron,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(cmd)
		}
	}
	return cmd
}

// ScheduleSyncOption customizes the sync command.
type ScheduleSyncOption func(*ScheduleSyncCommand)

// WithScheduleSyncCron overrides the cron expression for periodic reconciliation.
func WithScheduleSyncCron(expr string) ScheduleSyncOption {
	return func(cmd *ScheduleSyncCommand) {
		if expr != "" {
			cmd.cronExpr = expr
		}
	}
}

// WithScheduleSyncCLIName overrides the CLI command name.
func WithScheduleSyncCLIName(name string) ScheduleSyncOption {
	return func(cmd *ScheduleSyncCommand) {
		if name != "" {
			cmd.cliName = name
		}
	}
}

// WithScheduleSyncCLIGroup sets the CLI group.
func WithScheduleSyncCLIGroup(group string) ScheduleSyncOption {
	return func(cmd *ScheduleSyncCommand) {
		if group != "" {
			cmd.cliGroup = group
		}
	}
}

// WithScheduleSyncCLIDescription overrides the CLI description.
func WithScheduleSyncCLIDescription(desc string) ScheduleSyncOption {
	return func(cmd *ScheduleSyncCommand) {
		if desc != "" {
			cmd.cliDesc = desc
		}
	}
}

// CronHandler satisfies command.CronCommand to run periodic reconciliation.
func (c *ScheduleSyncCommand) CronHandler() func() error {
	return func() error {
		_, err := c.sync(context.Background())
		return err
	}
}

// CronOptions exposes the cron expression for the sync command.
func (c *ScheduleSyncCommand) CronOptions() command.HandlerConfig {
	return command.HandlerConfig{Expression: c.cronExpr}
}

// CLIHandler satisfies command.CLICommand to trigger reconciliation manually.
func (c *ScheduleSyncCommand) CLIHandler() any {
	return &scheduleSyncCLI{cmd: c}
}

// CLIOptions returns CLI metadata for registration.
func (c *ScheduleSyncCommand) CLIOptions() command.CLIConfig {
	return command.CLIConfig{
		Name:        c.cliName,
		Description: c.cliDesc,
		Group:       c.cliGroup,
	}
}

func (c *ScheduleSyncCommand) sync(ctx context.Context) (ReconcileResult, error) {
	if c.manager == nil {
		return ReconcileResult{}, fmt.Errorf("schedule manager not configured")
	}
	if c.loader == nil {
		return ReconcileResult{}, fmt.Errorf("schedule loader not configured")
	}

	defs, err := c.loader(ctx)
	if err != nil {
		return ReconcileResult{}, err
	}
	return c.manager.Reconcile(ctx, defs)
}

type scheduleSyncCLI struct {
	cmd *ScheduleSyncCommand

	From string `kong:"name='from',help='Path to JSON or YAML schedule definitions from settings'"`
}

// Run executes the reconciliation from CLI.
func (c *scheduleSyncCLI) Run() error {
	if c.cmd == nil {
		return fmt.Errorf("schedule sync command not configured")
	}

	ctx := context.Background()
	if strings.TrimSpace(c.From) != "" {
		defs, err := loadSchedulesFromFile(c.From)
		if err != nil {
			return err
		}
		_, err = c.cmd.manager.Reconcile(ctx, defs)
		return err
	}

	_, err := c.cmd.sync(ctx)
	return err
}

func loadSchedulesFromFile(path string) ([]ScheduleDefinition, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read schedules file: %w", err)
	}

	var defs []ScheduleDefinition
	if jsonErr := json.Unmarshal(content, &defs); jsonErr == nil {
		return defs, nil
	}

	if yamlErr := yaml.Unmarshal(content, &defs); yamlErr == nil {
		return defs, nil
	}

	return nil, fmt.Errorf("failed to parse schedules file %s as JSON or YAML", path)
}
