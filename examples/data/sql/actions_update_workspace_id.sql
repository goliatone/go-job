-- config
-- retry: 1
-- debug: true
UPDATE actions
SET workspace_id = (
  SELECT m.workspace_id FROM messages m WHERE m.id = actions.message_id
)
WHERE workspace_id IS NULL;
