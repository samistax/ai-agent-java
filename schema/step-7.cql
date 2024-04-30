ALTER TABLE datastax_ai_agent.network_traffic
    ADD (unit_id_ int, embedding vector<float,1536>);
CREATE CUSTOM INDEX IF NOT EXISTS network_traffic_embedding_idx
    ON datastax_ai_agent.network_traffic (embedding) USING 'StorageAttachedIndex';
CREATE CUSTOM INDEX IF NOT EXISTS network_traffic_unit_id_idx
    ON datastax_ai_agent.network_traffic (unit_id_) USING 'StorageAttachedIndex';
