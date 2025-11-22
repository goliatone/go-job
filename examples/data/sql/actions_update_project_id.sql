--- config
--- retry: 1
--- debug: true
--- timeout: 30s
--- schedule: "@every 10s"
UPDATE actions
SET project_id = (
  SELECT m.project_id FROM messages m WHERE m.id = actions.message_id
)
WHERE project_id IS NULL;
