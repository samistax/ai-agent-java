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

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.utility.DockerImageName;

abstract class CassandraIntegrationTest {
    private static CassandraContainer<?> database;

    @DynamicPropertySource
    static synchronized void databaseProperties(DynamicPropertyRegistry registry) {
        String port = System.getProperty("workshop.cassandra.port");
        if (null != port) {
            // The branch runner provisions one local database for all steps.
            registry.add("spring.cassandra.contact-points", () -> "127.0.0.1");
            registry.add("spring.cassandra.port", () -> port);
        } else {
            if (null == database) {
                database = new CassandraContainer<>(DockerImageName.parse("cassandra:5.0"))
                        .withNetworkMode("bridge")
                        .withInitScript("schema.cql");
                database.start();
                Runtime.getRuntime().addShutdownHook(new Thread(database::stop));
            }
            registry.add("spring.cassandra.contact-points", database::getHost);
            registry.add("spring.cassandra.port", () -> database.getMappedPort(9042));
        }
    }
}
