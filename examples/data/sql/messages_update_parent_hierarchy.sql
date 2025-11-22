--- config
--- retry: 1
--- debug: true
--- timeout: 30s
--- schedule: "@every 12s"
UPDATE messages
SET
    parent_id = (
        SELECT m2.id FROM messages m2 WHERE m2.message_id = messages.in_reply_to LIMIT 1
    )
WHERE
    parent_id IS NULL
    AND in_reply_to IS NOT NULL;

-- inherit project/workspace from parent when missing
UPDATE messages
SET
    project_id = (
        SELECT m2.project_id FROM messages m2 WHERE m2.id = messages.parent_id
    ),
    workspace_id = (
        SELECT m2.workspace_id FROM messages m2 WHERE m2.id = messages.parent_id
    )
WHERE
    parent_id IS NOT NULL
    AND (project_id IS NULL OR workspace_id IS NULL);
