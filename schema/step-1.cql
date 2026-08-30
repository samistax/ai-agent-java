CREATE TYPE IF NOT EXISTS datastax_ai_agent.ai_chat_message (
    msg_timestamp timestamp,
    msg_type      text,
    msg_content   text
);

CREATE TABLE IF NOT EXISTS datastax_ai_agent.agent_conversations (
    session_id        text,
    message_timestamp timestamp,
    messages          frozen<list<frozen<ai_chat_message>>>,
    PRIMARY KEY (session_id, message_timestamp)
  ) WITH CLUSTERING ORDER BY (message_timestamp DESC);
