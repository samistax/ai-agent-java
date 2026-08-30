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
import java.util.concurrent.CopyOnWriteArrayList;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import static org.assertj.core.api.Assertions.*;

@SpringBootTest(properties = {
    "spring.ai.openai.api-key=unused-local-test-key",
    "spring.ai.model.embedding=none",
    "spring.ai.vectorstore.cassandra.table=vector_store",
    "spring.ai.vectorstore.cassandra.initialize-schema=false",
    "spring.ai.chat.memory.repository.cassandra.initialize-schema=false"
})
@Import({BranchCompatibilityTest.LocalDatabase.class, SchemaDisabledTest.Tracking.class})
class SchemaDisabledTest extends CassandraIntegrationTest {
    @Autowired DdlTracker tracker;

    @TestConfiguration
    static class Tracking {
        @Bean DdlTracker ddlTracker() { return new DdlTracker(); }
        @Bean CqlSessionBuilderCustomizer trackDdl(DdlTracker tracker) {
            return builder -> builder.withRequestTracker(tracker);
        }
    }

    static class DdlTracker implements RequestTracker {
        final List<String> statements = new CopyOnWriteArrayList<>();
        private void record(Request request) {
            if (request instanceof SimpleStatement statement) {
                String query = statement.getQuery();
                if (query.stripLeading().toUpperCase(java.util.Locale.ROOT)
                        .matches("(?s)^(CREATE|ALTER|DROP)\\b.*")) {
                    statements.add(query);
                }
            }
        }
        @Override
        public void onSuccess(Request request, long latency, DriverExecutionProfile profile, Node node, String logPrefix) {
            record(request);
        }
        @Override
        public void onError(Request request, Throwable error, long latency, DriverExecutionProfile profile, Node node, String logPrefix) {
            record(request);
        }
        @Override public void close() {}
    }

    @Test
    void disablingSchemaInitializationEmitsNoDdl() {
        assertThat(tracker.statements).isEmpty();
    }
}
