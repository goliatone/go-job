--- schedule: "@every 30s"

SELECT
    id,
    message_id,
    parent_id,
    project_id,
    workspace_id,
    in_reply_to
FROM messages
ORDER BY id;
