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

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Pattern;

import com.datastax.ai.agent.broadbandStats.FccBroadbandDataService.DeviceTimeRange;
import com.datastax.ai.agent.broadbandStats.FccBroadbandDataService.BroadbandData;
import com.datastax.ai.agent.broadbandStats.FccBroadbandDataService.BroadbandReport;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import com.google.common.primitives.Floats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.ai.embedding.EmbeddingModel;


public final class FccBroadbandDataService implements Function<DeviceTimeRange, BroadbandReport> {

    private static final Logger logger = LoggerFactory.getLogger(FccBroadbandDataService.class);

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter HOUR_LABEL = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

    // windows up to this length are returned as the raw hourly samples, longer ones as daily summaries
    private static final long HOURLY_MAX_HOURS = 72;
    private static final long MAX_WINDOW_DAYS = 62;
    private static final int PEAKS_PER_COUNTER = 3;
    private static final double MB = 1_000_000d;

    // the ANN search is over-fetched, as the queried day itself (and duplicates) are dropped afterwards
    private static final int ANN_LIMIT = 25;
    private static final int SIMILAR_DAYS = 5;

    // one embedding describes one unit's traffic over one UTC day
    private static final String[] COUNTERS = {
        "sk_tx_bytes", "sk_rx_bytes", "cust_wired_tx_bytes", "cust_wired_rx_bytes", "cust_wifi_tx_bytes", "cust_wifi_rx_bytes"
    };

    // inputs per embeddings request (the OpenAI API accepts up to 2048)
    private static final int EMBEDDING_BATCH_SIZE = 100;

    private record UnitDay(int unit_id, LocalDate day) {}

    private record DayEmbedding(int unit_id, Instant lastSample, String description) {}

    /**
     * Parses an ISO-8601 local date-time, and also accepts the hour 24 (24:00 or 24:00:00) that LLMs write
     * for the end of a day, which is read as midnight at the start of the next day.
     */
    static final class LenientLocalDateTimeDeserializer extends StdDeserializer<LocalDateTime> {

        private static final Pattern END_OF_DAY = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})T24:00(?::00)?");

        LenientLocalDateTimeDeserializer() {
            super(LocalDateTime.class);
        }

        @Override
        public LocalDateTime deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            if (!parser.hasToken(JsonToken.VALUE_STRING)) {
                return (LocalDateTime) context.handleUnexpectedToken(LocalDateTime.class, parser);
            }
            String text = parser.getText().trim();
            try {
                var endOfDay = END_OF_DAY.matcher(text);
                return endOfDay.matches()
                        ? LocalDate.parse(endOfDay.group(1)).plusDays(1).atStartOfDay()
                        : LocalDateTime.parse(text);
            } catch (DateTimeParseException ex) {
                return (LocalDateTime) context.handleWeirdStringValue(LocalDateTime.class, text, "not an ISO-8601 local date-time, e.g. 2023-07-01T00:00:00");
            }
        }
    }

    public record TimeRange(
            @JsonPropertyDescription("Start of the window, inclusive. UTC, no zone suffix, e.g. 2023-07-01T00:00:00")
            @JsonDeserialize(using = LenientLocalDateTimeDeserializer.class)
            LocalDateTime from,
            @JsonPropertyDescription("End of the window, inclusive. UTC, no zone suffix. Use 23:59:59 for the end of a day, "
                    + "e.g. 2023-07-01T23:59:59; hour 24 is not valid")
            @JsonDeserialize(using = LenientLocalDateTimeDeserializer.class)
            LocalDateTime to) {}

    public record DeviceTimeRange(
            @JsonPropertyDescription("The device (unit) id")
            int device_id,
            TimeRange range) {}

    public record DeviceTimestamp(int device_id, LocalDateTime timestamp) {}

    public record BroadbandData(
            long sk_tx_bytes,
            long sk_rx_bytes,
            long cust_wired_tx_bytes,
            long cust_wired_rx_bytes,
            long cust_wifi_tx_bytes,
            long cust_wifi_rx_bytes) {}

    /** One period of traffic. All sizes are in megabytes (1 MB = 1,000,000 bytes). */
    public record Sample(
            String period,
            double sk_tx_mb,
            double sk_rx_mb,
            double cust_wired_tx_mb,
            double cust_wired_rx_mb,
            double cust_wifi_tx_mb,
            double cust_wifi_rx_mb) {}

    /** One of the largest hourly samples of a counter, and how it compares to the window's other samples. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Peak(
            String counter,
            String period,
            double mb,
            int share_of_window_percent,
            Double times_median) {}

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public record BroadbandReport(
            String message,
            int device_id,
            String first_sample,
            String last_sample,
            int rows,
            String granularity,
            Sample total,
            List<Sample> series,
            List<Sample> hour_of_day_average,
            List<Peak> peaks) {}

    public record SimilarDaysRequest(
            @JsonPropertyDescription("The device (unit) id")
            int device_id,
            @JsonPropertyDescription("The day to find similar days for, UTC, e.g. 2023-07-01")
            String date) {}

    public record SimilarDay(int unit_id, String date, double cosine_similarity, String description) {}

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public record SimilarDays(String message, String day_description, List<SimilarDay> matches) {}

    private record Reading(Instant time, long[] bytes) {}

    private final CqlSession cqlSession;
    private final EmbeddingModel embeddingClient;

    // LocalDateTime values (from the LLM and the CSV) are zone-less and always interpreted as UTC,
    //  the dtime column is a timestamp, which the driver maps to Instant
    private static Instant toInstant(LocalDateTime dateTime) {
        return dateTime.toInstant(ZoneOffset.UTC);
    }

    FccBroadbandDataService(CqlSession cqlSession, EmbeddingModel embeddingModel, boolean initializeSchema) {
        this.cqlSession = cqlSession;
        this.embeddingClient = embeddingModel;
        if (initializeSchema) {
            addEmbeddingColumnAndIndex(cqlSession, embeddingModel);
        }
    }

    @Override
    public BroadbandReport apply(DeviceTimeRange deviceTimeRange) {
        return read(deviceTimeRange);
    }

    /**
     * Reads a device's traffic over a time window.
     * Windows up to three days return the hourly samples. Longer windows return one entry per day,
     * plus the average per hour of the day, which shows recurring patterns.
     * Either way the report has the window's totals and its largest hourly samples per counter.
     */
    public BroadbandReport read(DeviceTimeRange q) {

        if (null == q || null == q.range() || null == q.range().from() || null == q.range().to()) {
            return message(null == q ? 0 : q.device_id(), "Both range.from and range.to are required, e.g. 2023-07-01T00:00:00 (UTC).");
        }
        Instant from = toInstant(q.range().from());
        Instant to = toInstant(q.range().to());
        Duration window = Duration.between(from, to);

        if (window.isNegative() || window.isZero()) {
            return message(q.device_id(), "range.to must be after range.from.");
        }
        if (window.toDays() > MAX_WINDOW_DAYS) {
            return message(q.device_id(), "The window is too long, ask for at most " + MAX_WINDOW_DAYS + " days at a time.");
        }

        SimpleStatement stmt = QueryBuilder.selectFrom("datastax_ai_agent", "network_traffic")
                .columns(COUNTERS)
                .column("dtime")
                .whereColumn("unit_id").isEqualTo(QueryBuilder.literal(q.device_id()))
                .whereColumn("dtime").isGreaterThanOrEqualTo(QueryBuilder.literal(from))
                .whereColumn("dtime").isLessThanOrEqualTo(QueryBuilder.literal(to))
                .build();

        List<Reading> readings = new ArrayList<>();
        for (Row r : cqlSession.execute(stmt)) {
            long[] bytes = new long[COUNTERS.length];
            for (int c = 0; c < COUNTERS.length; ++c) {
                bytes[c] = r.getLong(COUNTERS[c]);
            }
            readings.add(new Reading(r.getInstant("dtime"), bytes));
        }
        if (readings.isEmpty()) {
            return noData(q.device_id(), from, to);
        }

        long[] total = new long[COUNTERS.length];
        for (Reading reading : readings) {
            add(total, reading.bytes());
        }

        boolean hourly = window.toHours() <= HOURLY_MAX_HOURS;
        List<Sample> series = new ArrayList<>();
        List<Sample> hourOfDay = new ArrayList<>();

        if (hourly) {
            for (Reading reading : readings) {
                series.add(sample(hourLabel(reading.time()), reading.bytes()));
            }
        } else {
            Map<LocalDate,long[]> perDay = new TreeMap<>();
            long[][] perHour = new long[24][COUNTERS.length];
            int[] perHourCount = new int[24];

            for (Reading reading : readings) {
                var utc = reading.time().atOffset(ZoneOffset.UTC);
                add(perDay.computeIfAbsent(utc.toLocalDate(), d -> new long[COUNTERS.length]), reading.bytes());
                add(perHour[utc.getHour()], reading.bytes());
                ++perHourCount[utc.getHour()];
            }
            perDay.forEach((day, bytes) -> series.add(sample(day.toString(), bytes)));

            for (int h = 0; h < 24; ++h) {
                if (0 < perHourCount[h]) {
                    long[] average = new long[COUNTERS.length];
                    for (int c = 0; c < COUNTERS.length; ++c) {
                        average[c] = perHour[h][c] / perHourCount[h];
                    }
                    hourOfDay.add(sample(String.format("%02d:00", h), average));
                }
            }
        }

        return new BroadbandReport(
                "Sizes are in MB (1 MB = 1,000,000 bytes), times are UTC. Each sample covers one hour. "
                        + (hourly ? "The series has the hourly samples." : "The series has one entry per day; hour_of_day_average is the mean per sample at that hour of the day."),
                q.device_id(),
                hourLabel(readings.get(0).time()),
                hourLabel(readings.get(readings.size() - 1).time()),
                readings.size(),
                hourly ? "hourly" : "daily",
                sample("total", total),
                series,
                hourOfDay,
                peaks(readings, total));
    }

    private static BroadbandReport message(int deviceId, String message) {
        return new BroadbandReport(message, deviceId, null, null, 0, null, null, List.of(), List.of(), List.of());
    }

    /** An empty window usually means the wrong dates were asked for, so tell what the unit does have. */
    private BroadbandReport noData(int deviceId, Instant from, Instant to) {

        SimpleStatement firstStmt = QueryBuilder.selectFrom("datastax_ai_agent", "network_traffic")
                .column("dtime")
                .whereColumn("unit_id").isEqualTo(QueryBuilder.literal(deviceId))
                .limit(1)
                .build();

        SimpleStatement lastStmt = QueryBuilder.selectFrom("datastax_ai_agent", "network_traffic")
                .column("dtime")
                .whereColumn("unit_id").isEqualTo(QueryBuilder.literal(deviceId))
                .orderBy("dtime", ClusteringOrder.DESC)
                .limit(1)
                .build();

        Row first = cqlSession.execute(firstStmt).one();
        Row last = cqlSession.execute(lastStmt).one();

        if (null == first || null == last) {
            return message(deviceId, "There is no data at all for unit " + deviceId + ".");
        }
        return message(deviceId,
                "No samples for unit " + deviceId + " between " + hourLabel(from) + " and " + hourLabel(to)
                        + " (UTC). Data for this unit exists from " + hourLabel(first.getInstant("dtime"))
                        + " to " + hourLabel(last.getInstant("dtime"))
                        + " (UTC), ask for a window inside that range.");
    }

    private static void add(long[] sum, long[] bytes) {
        for (int c = 0; c < sum.length; ++c) {
            sum[c] += bytes[c];
        }
    }

    private static String hourLabel(Instant time) {
        return HOUR_LABEL.format(time.atOffset(ZoneOffset.UTC));
    }

    private static double mb(long bytes) {
        return Math.round(bytes / MB * 100) / 100.0;
    }

    private static Sample sample(String period, long[] b) {
        return new Sample(period, mb(b[0]), mb(b[1]), mb(b[2]), mb(b[3]), mb(b[4]), mb(b[5]));
    }

    /**
     * The largest hourly samples per counter. The data is very uneven, a few hours often carry most of the traffic,
     * so each peak also tells its share of the window's total and how many times the median sample it is.
     */
    private static List<Peak> peaks(List<Reading> readings, long[] total) {

        List<Peak> peaks = new ArrayList<>();
        for (int c = 0; c < COUNTERS.length; ++c) {
            if (0 == total[c]) {
                continue;
            }
            final int counter = c;
            long[] sorted = readings.stream().mapToLong(r -> r.bytes()[counter]).sorted().toArray();
            double median = 1 == sorted.length % 2
                    ? sorted[sorted.length / 2]
                    : (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2.0;

            readings.stream()
                    .sorted(Comparator.comparingLong((Reading r) -> r.bytes()[counter]).reversed())
                    .limit(PEAKS_PER_COUNTER)
                    .forEach(r -> peaks.add(new Peak(
                            COUNTERS[counter].replace("_bytes", "_mb"),
                            hourLabel(r.time()),
                            mb(r.bytes()[counter]),
                            (int) Math.round(100.0 * r.bytes()[counter] / total[counter]),
                            0 < median ? Math.round(r.bytes()[counter] / median * 10) / 10.0 : null)));
        }
        return peaks;
    }

    /**
     * Finds days whose traffic has a similar shape to the given unit's day, using the daily embeddings.
     */
    public SimilarDays findSimilarDays(SimilarDaysRequest request) {

        LocalDate day;
        try {
            day = LocalDate.parse(request.date().strip().substring(0, 10));
        } catch (RuntimeException ex) {
            return new SimilarDays("date must be a day like 2023-07-01.", null, List.of());
        }
        UnitDay queried = new UnitDay(request.device_id(), day);

        DayEmbedding queriedDay = describeDay(queried);
        if (null == queriedDay) {
            return new SimilarDays(
                    "There is no data for unit " + request.device_id() + " on " + day
                            + " (UTC). Use the traffic tool to see which dates the unit has data for.",
                    null,
                    List.of());
        }

        SimpleStatement stmt = QueryBuilder.selectFrom("datastax_ai_agent", "network_traffic")
                .columns("dtime", "embedding")
                .whereColumn("unit_id").isEqualTo(QueryBuilder.literal(queried.unit_id()))
                .whereColumn("dtime").isGreaterThanOrEqualTo(QueryBuilder.literal(toInstant(day.atStartOfDay())))
                .whereColumn("dtime").isLessThan(QueryBuilder.literal(toInstant(day.plusDays(1).atStartOfDay())))
                .build();

        CqlVector<Float> embedding = null;
        for (Row r : cqlSession.execute(stmt)) {
            CqlVector<Float> candidate = r.getVector("embedding", Float.class);
            if (null != candidate) {
                embedding = candidate;
            }
        }
        if (null == embedding) {
            return new SimilarDays(
                    "Unit " + request.device_id() + " has data on " + day + " but no daily embedding was stored for it.",
                    queriedDay.description(),
                    List.of());
        }

        stmt = QueryBuilder.selectFrom("datastax_ai_agent", "network_traffic")
                .columns("unit_id", "dtime", "embedding")
                .orderByAnnOf("embedding", embedding)
                .limit(ANN_LIMIT)
                .build();

        Set<UnitDay> seen = new LinkedHashSet<>();
        seen.add(queried);
        List<SimilarDay> matches = new ArrayList<>();

        for (Row r : cqlSession.execute(stmt)) {
            if (SIMILAR_DAYS <= matches.size()) {
                break;
            }
            UnitDay unitDay = new UnitDay(r.getInt("unit_id"), r.getInstant("dtime").atOffset(ZoneOffset.UTC).toLocalDate());
            if (!seen.add(unitDay)) {
                continue;
            }
            DayEmbedding dayEmbedding = describeDay(unitDay);
            if (null != dayEmbedding) {
                matches.add(new SimilarDay(
                        unitDay.unit_id(),
                        unitDay.day().toString(),
                        Math.round(cosine(embedding, r.getVector("embedding", Float.class)) * 1000) / 1000.0,
                        dayEmbedding.description()));
            }
        }
        return new SimilarDays(
                matches.isEmpty() ? "No other days with a stored embedding were found." : null,
                queriedDay.description(),
                matches);
    }

    private static double cosine(CqlVector<Float> a, CqlVector<Float> b) {

        double dot = 0;
        double normA = 0;
        double normB = 0;
        for (int i = 0; i < Math.min(a.size(), b.size()); ++i) {
            double x = a.get(i);
            double y = b.get(i);
            dot += x * y;
            normA += x * x;
            normB += y * y;
        }
        return 0 == normA || 0 == normB ? 0 : dot / Math.sqrt(normA * normB);
    }

    // TODO write a script that reads the csv row-by-row and calls this
    public void write(DeviceTimestamp deviceTimestamp, BroadbandData broadbandData) {

        // write the data
        SimpleStatement stmt = QueryBuilder.insertInto("datastax_ai_agent", "network_traffic")
                .value("unit_id", QueryBuilder.literal(deviceTimestamp.device_id))
                .value("unit_id_", QueryBuilder.literal(deviceTimestamp.device_id))
                .value("dtime", QueryBuilder.literal(toInstant(deviceTimestamp.timestamp)))
                .value("sk_tx_bytes", QueryBuilder.literal(broadbandData.sk_tx_bytes))
                .value("sk_rx_bytes", QueryBuilder.literal(broadbandData.sk_rx_bytes))
                .value("cust_wired_tx_bytes", QueryBuilder.literal(broadbandData.cust_wired_tx_bytes))
                .value("cust_wired_rx_bytes", QueryBuilder.literal(broadbandData.cust_wired_rx_bytes))
                .value("cust_wifi_tx_bytes", QueryBuilder.literal(broadbandData.cust_wifi_tx_bytes))
                .value("cust_wifi_rx_bytes", QueryBuilder.literal(broadbandData.cust_wifi_rx_bytes))
                .build();

        cqlSession.execute(stmt);
    }

    /**
     * Creates and stores the daily embeddings for the given unit-days, as one batch.
     * Each embedding is stored on the unit-day's last row, so no extra rows are created.
     * The unit-days that got an embedding are added to {@code embedded}.
     */
    private void embedDays(Collection<UnitDay> days, Set<UnitDay> embedded) {

        List<UnitDay> described = new ArrayList<>();
        List<DayEmbedding> batch = new ArrayList<>();
        for (UnitDay unitDay : days) {
            DayEmbedding dayEmbedding = describeDay(unitDay);
            if (null != dayEmbedding) {
                described.add(unitDay);
                batch.add(dayEmbedding);
            } else {
                logger.warn("No rows found when reading back unit {} on {}, so no embedding is created for it", unitDay.unit_id(), unitDay.day());
            }
        }
        embedAndStore(batch);
        embedded.addAll(described);
    }

    private void embedAndStore(List<DayEmbedding> batch) {

        if (batch.isEmpty()) {
            return;
        }
        List<float[]> embeddings = embeddingClient.embed(batch.stream().map(DayEmbedding::description).toList());

        for (int i = 0; i < batch.size(); ++i) {
            DayEmbedding d = batch.get(i);
            SimpleStatement stmt = QueryBuilder.insertInto("datastax_ai_agent", "network_traffic")
                    .value("unit_id", QueryBuilder.literal(d.unit_id))
                    .value("dtime", QueryBuilder.literal(d.lastSample))
                    .value("embedding", QueryBuilder.literal(CqlVector.newInstance(Floats.asList(embeddings.get(i)))))
                    .build();

            cqlSession.execute(stmt);
        }
    }

    /**
     * Describes the shape of a unit's day in a short text: per counter, the day's total
     * and how much of it falls in the two busiest hours (UTC).
     * Unit id and date are left out on purpose, so that days with a similar shape are close to each other.
     */
    private DayEmbedding describeDay(UnitDay unitDay) {

        SimpleStatement stmt = QueryBuilder.selectFrom("datastax_ai_agent", "network_traffic")
                .columns(COUNTERS)
                .column("dtime")
                .whereColumn("unit_id").isEqualTo(QueryBuilder.literal(unitDay.unit_id))
                .whereColumn("dtime").isGreaterThanOrEqualTo(QueryBuilder.literal(toInstant(unitDay.day.atStartOfDay())))
                .whereColumn("dtime").isLessThan(QueryBuilder.literal(toInstant(unitDay.day.plusDays(1).atStartOfDay())))
                .build();

        long[][] perHour = new long[COUNTERS.length][24];
        Instant lastSample = null;
        for (Row r : cqlSession.execute(stmt)) {
            Instant dtime = r.getInstant("dtime");
            int hour = dtime.atOffset(ZoneOffset.UTC).getHour();
            for (int c = 0; c < COUNTERS.length; ++c) {
                perHour[c][hour] += r.getLong(COUNTERS[c]);
            }
            if (null == lastSample || dtime.isAfter(lastSample)) {
                lastSample = dtime;
            }
        }
        if (null == lastSample) {
            return null;
        }

        StringBuilder description = new StringBuilder("Daily network traffic by hour (UTC).");
        for (int c = 0; c < COUNTERS.length; ++c) {
            long total = Arrays.stream(perHour[c]).sum();
            description.append(' ').append(COUNTERS[c].replace("cust_", "").replace("_bytes", "")).append(": ");
            if (0 == total) {
                description.append("no traffic.");
                continue;
            }
            int first = 0;
            int second = -1;
            for (int h = 1; h < 24; ++h) {
                if (perHour[c][h] > perHour[c][first]) {
                    second = first;
                    first = h;
                } else if (-1 == second || perHour[c][h] > perHour[c][second]) {
                    second = h;
                }
            }
            if (-1 == second) {
                second = 0 == first ? 1 : 0;
            }
            long busiest = perHour[c][first] + perHour[c][second];
            description.append(String.format(
                    "total %s, busiest hours %02d:00 and %02d:00 carry %d%%.",
                    humanReadable(total), Math.min(first, second), Math.max(first, second), 100 * busiest / total));
        }
        return new DayEmbedding(unitDay.unit_id, lastSample, description.toString());
    }

    private static String humanReadable(long bytes) {
        if (bytes >= 1_000_000_000L) {
            return String.format("%.1f GB", bytes / 1e9);
        }
        if (bytes >= 1_000_000L) {
            return String.format("%.0f MB", bytes / 1e6);
        }
        return String.format("%d KB", bytes / 1_000L);
    }

    /** Receives the progress of an import, and can stop it. The methods are called from the importing thread. */
    public interface ImportProgress {

        /** @param bytesRead how far into the file the import is, approximately */
        default void rows(long rows, long skipped, long bytesRead) {}

        default void embeddings(int daysDone, int daysTotal) {}

        default boolean cancelled() {
            return false;
        }
    }

    /** @param firstError the first line that was skipped and why, or null */
    public record ImportResult(long rows, long skipped, int units, int days, int embeddings, boolean cancelled, String firstError) {}

    // how often the progress is reported, in rows
    private static final int PROGRESS_INTERVAL_ROWS = 500;

    /**
     * Imports a CSV file with the columns
     * unit_id,dtime,sk_tx_bytes,sk_rx_bytes,cust_wired_tx_bytes,cust_wired_rx_bytes,cust_wifi_tx_bytes,cust_wifi_rx_bytes
     * where dtime looks like 2023-07-01 05:46:44 (UTC).
     * Lines that can not be parsed are skipped and counted.
     * Rows are stored by (unit_id, dtime), so importing the same file again overwrites instead of duplicating.
     * <p>
     * The daily embedding of a unit-day is created as soon as the file moves on to another unit-day,
     * so when the file is ordered by unit and time, a cancelled import keeps the embeddings of the days read so far.
     * A unit-day that shows up again later (an unordered file) is embedded again at the end.
     */
    public ImportResult importCsv(Path csvFile, ImportProgress progress) throws IOException {

        long rows = 0;
        long skipped = 0;
        long bytesRead = 0;
        long lineNumber = 1; // the header
        String firstError = null;

        // the unit-days that got rows, in the order they were first seen
        Set<UnitDay> unitDays = new LinkedHashSet<>();
        // the unit-days that have an embedding which is up to date with their rows
        Set<UnitDay> embedded = new HashSet<>();
        // the unit-days that the file has moved past, waiting for their embedding
        Set<UnitDay> ready = new LinkedHashSet<>();
        UnitDay current = null;

        try (BufferedReader br = Files.newBufferedReader(csvFile)) {
            String line = br.readLine(); //skip header
            while (!progress.cancelled() && (line = br.readLine()) != null) {
                ++lineNumber;
                bytesRead += line.length() + 1;
                if (line.isBlank()) {
                    continue;
                }
                try {
                    // Use comma as separator
                    String[] columns = line.split(",");

                    LocalDateTime dateTime = LocalDateTime.parse(columns[1].strip(), FORMATTER);

                    FccBroadbandDataService.DeviceTimestamp device
                            = new FccBroadbandDataService.DeviceTimestamp(
                                Integer.parseInt(columns[0].strip()),
                                dateTime);

                    FccBroadbandDataService.BroadbandData data = new FccBroadbandDataService.BroadbandData(
                            Long.parseLong(columns[2].strip()),
                            Long.parseLong(columns[3].strip()),
                            Long.parseLong(columns[4].strip()),
                            Long.parseLong(columns[5].strip()),
                            Long.parseLong(columns[6].strip()),
                            Long.parseLong(columns[7].strip()));

                    write(device, data);
                    UnitDay unitDay = new UnitDay(device.device_id, dateTime.toLocalDate());
                    unitDays.add(unitDay);
                    ++rows;

                    if (!unitDay.equals(current)) {
                        // the file has moved on, so the previous day is complete
                        if (null != current) {
                            ready.add(current);
                        }
                        current = unitDay;
                        // a day that is seen again has new rows, its earlier embedding is out of date
                        ready.remove(unitDay);
                        embedded.remove(unitDay);
                        if (ready.size() >= EMBEDDING_BATCH_SIZE) {
                            embedDays(ready, embedded);
                            ready.clear();
                        }
                    }
                } catch (RuntimeException ex) {
                    // bad line, e.g. missing column, text where a number is expected, or an unexpected date format
                    ++skipped;
                    if (null == firstError) {
                        firstError = "line " + lineNumber;
                    }
                    logger.debug("skipping line {}", lineNumber, ex);
                }
                if (0 == (rows + skipped) % PROGRESS_INTERVAL_ROWS) {
                    progress.rows(rows, skipped, bytesRead);
                }
            }
        }
        progress.rows(rows, skipped, bytesRead);

        if (!progress.cancelled() && null != current) {
            // the last day of the file is complete too
            ready.add(current);
        }
        // when cancelled, only the days that were read completely get an embedding
        embedDays(ready, embedded);

        if (!progress.cancelled()) {
            // normally nothing is left, but more when the file is not ordered by unit and time
            List<UnitDay> missing = unitDays.stream().filter(d -> !embedded.contains(d)).toList();
            for (int from = 0; from < missing.size() && !progress.cancelled(); from += EMBEDDING_BATCH_SIZE) {
                int to = Math.min(from + EMBEDDING_BATCH_SIZE, missing.size());
                embedDays(missing.subList(from, to), embedded);
                progress.embeddings(to, missing.size());
            }
        }
        logger.info("Stored {} rows ({} skipped) and {} daily embeddings for {} unit-days",
                rows, skipped, embedded.size(), unitDays.size());
        return new ImportResult(
                rows,
                skipped,
                (int) unitDays.stream().map(UnitDay::unit_id).distinct().count(),
                unitDays.size(),
                embedded.size(),
                progress.cancelled(),
                firstError);
    }

    private static void addEmbeddingColumnAndIndex(CqlSession cqlSession, EmbeddingModel embeddingModel) {

        // hacky schema changes
        try {
            cqlSession.execute(
                    String.format(
                            "ALTER TABLE datastax_ai_agent.network_traffic ADD (unit_id_ int,embedding vector<float,%s>)",
                            String.valueOf(embeddingModel.dimensions())));

        } catch (InvalidQueryException ex) {
            logger.info(ex.getLocalizedMessage());
        }
        try {
            cqlSession.execute(
                            "CREATE CUSTOM INDEX IF NOT EXISTS network_traffic_embedding_idx ON datastax_ai_agent.network_traffic (embedding) USING 'StorageAttachedIndex'");

            // copy this column and put a SAI on it so we can `WHERE !=` on it
            cqlSession.execute(
                            "CREATE CUSTOM INDEX IF NOT EXISTS network_traffic_unit_id_idx ON datastax_ai_agent.network_traffic (unit_id_) USING 'StorageAttachedIndex'");

        } catch (InvalidQueryException ex) {
            logger.info(ex.getLocalizedMessage());
        }
    }
}
