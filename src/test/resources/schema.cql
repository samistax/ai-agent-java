CREATE KEYSPACE IF NOT EXISTS datastax_ai_agent
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

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

CREATE TABLE IF NOT EXISTS datastax_ai_agent.vector_store (
    id text PRIMARY KEY,
    content text,
    embedding vector<float,1536>
);
CREATE CUSTOM INDEX IF NOT EXISTS vector_store_embedding_idx
    ON datastax_ai_agent.vector_store (embedding) USING 'StorageAttachedIndex';

ALTER TABLE datastax_ai_agent.agent_conversations
    ADD (prompt_request text, assistant text, embedding vector<float,1536>);
CREATE CUSTOM INDEX IF NOT EXISTS agent_conversations_embedding_idx
    ON datastax_ai_agent.agent_conversations (embedding) USING 'StorageAttachedIndex';

CREATE TABLE IF NOT EXISTS datastax_ai_agent.network_traffic (
    unit_id int,
    dtime timestamp,
    sk_tx_bytes bigint,
    sk_rx_bytes bigint,
    cust_wired_tx_bytes bigint,
    cust_wired_rx_bytes bigint,
    cust_wifi_tx_bytes bigint,
    cust_wifi_rx_bytes bigint,
    PRIMARY KEY (unit_id, dtime)
);

ALTER TABLE datastax_ai_agent.network_traffic
    ADD (unit_id_ int, embedding vector<float,1536>);
CREATE CUSTOM INDEX IF NOT EXISTS network_traffic_embedding_idx
    ON datastax_ai_agent.network_traffic (embedding) USING 'StorageAttachedIndex';
CREATE CUSTOM INDEX IF NOT EXISTS network_traffic_unit_id_idx
    ON datastax_ai_agent.network_traffic (unit_id_) USING 'StorageAttachedIndex';
