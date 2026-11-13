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
package com.datastax.ai.agent.broadbandStats;

import java.util.function.Function;

import com.datastax.ai.agent.broadbandStats.FccBroadbandDataService.SimilarDays;
import com.datastax.ai.agent.broadbandStats.FccBroadbandDataService.SimilarDaysRequest;
import com.datastax.oss.driver.api.core.CqlSession;

import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Description;

@Configuration
public class FccBroadbandDataTool {

    @Bean
    @Description("Get a device's network traffic over a time window (at most 62 days). "
            + "Times are ISO-8601 local date-times in UTC without a zone suffix, e.g. 2023-07-01T00:00:00, and the hour is 00-23, never 24. "
            + "Both ends of the window are inclusive, so write the end of a day as 23:59:59, e.g. the evening 18:00-24:00 is 18:00:00 to 23:59:59. "
            + "To compare periods, such as morning and evening, call this once for each period. "
            + "The data is hourly, one sample per hour, and covers 2023 only, so always ask for explicit dates and never guess from today's date. "
            + "Windows up to 3 days return the hourly samples (use them to find peaks and busy periods), "
            + "longer windows return one entry per day plus the average per hour of the day. "
            + "Sizes are in MB. The counters: sk_tx/sk_rx are sent/received by the device itself, "
            + "cust_wired_tx/rx and cust_wifi_tx/rx are the customer traffic over the wired and WiFi connections. "
            + "If the window has no data, the result tells which dates the device does have.")
    public FccBroadbandDataService fccBroadbandDataService(
            CqlSession cqlSession,
            EmbeddingModel embeddingModel,
            @Value("${spring.ai.vectorstore.cassandra.initialize-schema:false}") boolean initializeSchema) {

        return new FccBroadbandDataService(cqlSession, embeddingModel, initializeSchema);
    }

    @Bean
    @Description("Find days whose network traffic has a similar daily pattern to a given device's day, "
            + "across all devices and dates. The date is a UTC day like 2023-07-01. "
            + "Returns the most similar days with their cosine similarity and a description of each day. "
            + "Use the traffic tool afterwards to compare the hours of the days in detail.")
    public Function<SimilarDaysRequest, SimilarDays> fccSimilarDaysService(FccBroadbandDataService service) {
        return service::findSimilarDays;
    }
}
