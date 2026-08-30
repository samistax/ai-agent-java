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

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.memory.repository.cassandra.CassandraChatMemoryRepository;
import org.springframework.ai.chat.memory.repository.cassandra.CassandraChatMemoryRepositoryConfig;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = {
    "spring.ai.openai.api-key=unused-local-test-key",
    "spring.ai.model.embedding=none",
    "spring.ai.vectorstore.cassandra.table=vector_store",
    "spring.ai.chat.memory.repository.cassandra.initialize-schema=false"
})
@Import(BranchCompatibilityTest.LocalDatabase.class)
class BranchCompatibilityTest extends CassandraIntegrationTest {
    @Autowired CqlSession session;
    @Autowired CassandraChatMemoryRepository repository;

    @TestConfiguration
    static class LocalDatabase {
        @Bean
        org.springframework.boot.autoconfigure.cassandra.DriverConfigLoaderBuilderCustomizer protocolV4() {
            return builder -> builder.withString(DefaultDriverOption.PROTOCOL_VERSION, "V4");
        }

        @Bean
        @Primary
        EmbeddingModel localEmbeddingModel() {
            EmbeddingModel model = mock(EmbeddingModel.class);
            when(model.dimensions()).thenReturn(1536);
            when(model.embed(anyString())).thenAnswer(invocation -> vector());
            when(model.embed(anyList(), any(), any())).thenAnswer(invocation -> {
                List<?> documents = invocation.getArgument(0);
                return documents.stream().map(ignored -> vector()).toList();
            });
            return model;
        }
    }

    private static float[] vector() {
        float[] vector = new float[1536];
        vector[0] = 1;
        return vector;
    }

    @Test
    void manualSchemaRoundTripsMessagesAcrossRepositoryRestart() {
        assertThat(session.getContext().getProtocolVersion()).isEqualTo(DefaultProtocolVersion.V4);
        String id = "branch-test-" + UUID.randomUUID();
        UserMessage question = new UserMessage("Remember this branch test");
        AssistantMessage answer = new AssistantMessage("Remembered");
        Instant timestamp = Instant.now();
        question.getMetadata().put(CassandraChatMemoryRepository.CONVERSATION_TS, timestamp);
        answer.getMetadata().put(CassandraChatMemoryRepository.CONVERSATION_TS, timestamp);
        repository.saveAll(id, List.of(question, answer));
        var reopened = CassandraChatMemoryRepository.create(config().disallowSchemaChanges().build());
        assertThat(reopened.findByConversationId(id)).extracting(m -> m.getText())
                .containsExactly("Remember this branch test", "Remembered");
        assertThat(reopened.findByConversationId("absent-" + UUID.randomUUID())).isEmpty();
    }

    @Test
    void disabledInitializationDoesNotCreateMissingTable() {
        String table = "missing_" + UUID.randomUUID().toString().replace("-", "");
        assertThatThrownBy(() -> CassandraChatMemoryRepository.create(
                config().withTableName(table).disallowSchemaChanges().build()))
                .isInstanceOf(RuntimeException.class);
        assertThat(session.getMetadata().getKeyspace("datastax_ai_agent").orElseThrow()
                .getTable(table)).isEmpty();
    }

    @Test
    void enabledInitializationReproducesProtocolV4Failure() {
        assertThatThrownBy(() -> CassandraChatMemoryRepository.create(config().build()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Can't use per-request keyspace with protocol V4");
    }

    @Test
    void messagesSurviveTheNextWorkshopBranch() {
        int step = Integer.getInteger("workshop.step", 1);
        String id = System.getProperty("workshop.run", "workshop-transition");
        if (1 < step) {
            assertThat(repository.findByConversationId(id)).extracting(m -> m.getText())
                    .containsExactly("step-" + (step - 1));
        }
        UserMessage message = new UserMessage("step-" + step);
        message.getMetadata().put(CassandraChatMemoryRepository.CONVERSATION_TS, Instant.now());
        repository.saveAll(id, List.of(message));
    }

    private CassandraChatMemoryRepositoryConfig.Builder config() {
        return CassandraChatMemoryRepositoryConfig.builder().withCqlSession(session)
                .withKeyspaceName("datastax_ai_agent").withTableName("agent_conversations");
    }
}
