/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.
 */
package com.datastax.ai.agent;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.cassandra.CassandraVectorStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import static org.assertj.core.api.Assertions.*;

@SpringBootTest(properties = {
    "spring.ai.openai.api-key=unused-local-test-key",
    "spring.ai.model.embedding=none",
    "spring.ai.vectorstore.cassandra.table=vector_store",
    "spring.ai.vectorstore.cassandra.initialize-schema=false",
    "spring.ai.chat.memory.repository.cassandra.initialize-schema=false"
})
@Import(BranchCompatibilityTest.LocalDatabase.class)
class VectorCompatibilityTest extends CassandraIntegrationTest {
    @Autowired CassandraVectorStore store;

    @Test
    void manualSchemaSupportsVectorWriteSearchAndDelete() {
        Document document = new Document(UUID.randomUUID().toString(), "local vector test", java.util.Map.of());
        try {
            store.add(List.of(document));
            assertThat(store.similaritySearch(SearchRequest.builder().query("local vector test").topK(10).build()))
                    .extracting(Document::getId).contains(document.getId());
        } finally {
            store.delete(List.of(document.getId()));
        }
    }
}
