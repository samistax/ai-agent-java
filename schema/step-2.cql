CREATE TABLE IF NOT EXISTS datastax_ai_agent.vector_store (
    id text PRIMARY KEY,
    content text,
    embedding vector<float,1536>
);
CREATE CUSTOM INDEX IF NOT EXISTS vector_store_embedding_idx
    ON datastax_ai_agent.vector_store (embedding) USING 'StorageAttachedIndex';
