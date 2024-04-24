ALTER TABLE datastax_ai_agent.agent_conversations
    ADD (prompt_request text, assistant text, embedding vector<float,1536>);
CREATE CUSTOM INDEX IF NOT EXISTS agent_conversations_embedding_idx
    ON datastax_ai_agent.agent_conversations (embedding) USING 'StorageAttachedIndex';
