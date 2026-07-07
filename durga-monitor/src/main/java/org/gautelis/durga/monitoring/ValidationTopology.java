package org.gautelis.durga.monitoring;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.gautelis.durga.ProcessEvent;
import org.gautelis.durga.validation.ValidationResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Kafka Streams read model for validation mode.
 * <p>
 * Pairs the validation-process shadow output (the ACTIVITY_COMPLETED lifecycle event from the
 * validation events stream) against the prior/production output for the same activity+instance
 * from the production events stream, and computes a {@link ValidationResult} per task per instance.
 * <p>
 * Both sides carry identical identity keys ({@code processId:activityId:processInstanceId}) and are
 * merged into a single aggregate so the comparison is robust to arrival order: in live-concurrent
 * mode the validation event may arrive before or after the production event.
 */
public final class ValidationTopology {

    public static final String DEFAULT_CANDIDATE_EVENTS_PATTERN = "process-events-.*-validation";
    public static final String DEFAULT_RESULTS_TOPIC = "validation-results";
    public static final String DEFAULT_RESULTS_STORE = "validation-results-store";

    private static final String LOCAL_JOIN_STORE = "validation-join-store";

    private ValidationTopology() {
    }

    /**
     * Builds the validation comparison topology.
     *
     * @param topics topic, store, and ignore-path configuration
     * @return topology that joins prior (production) and validation (candidate) output and materializes results
     */
    public static Topology buildTopology(ValidationTopics topics) {
        StreamsBuilder builder = new StreamsBuilder();
        var eventSerde = JsonSerde.forClass(ProcessEvent.class);
        var joinInputSerde = JsonSerde.forClass(JoinInput.class);
        var joinStateSerde = JsonSerde.forClass(ValidationJoinState.class);
        var resultSerde = JsonSerde.forClass(ValidationResult.class);

        // Production events (excludes validation-event topics).
        KStream<String, ProcessEvent> productionEvents;
        if (topics.eventsPattern() != null) {
            productionEvents = builder.stream(topics.eventsPattern(), Consumed.with(Serdes.String(), eventSerde));
        } else {
            productionEvents = builder.stream(topics.eventsTopic(), Consumed.with(Serdes.String(), eventSerde));
        }

        KStream<String, JoinInput> priorInputs = productionEvents
                .filter((key, event) -> event != null
                        && event.eventType() == ProcessEvent.EventType.ACTIVITY_COMPLETED
                        && event.processId() != null && event.activityId() != null
                        && event.processInstanceId() != null)
                .map((key, event) -> KeyValue.pair(priorKey(event), JoinInput.prior(event)));

        // Validation (candidate) events from the per-process shadow stream.
        KStream<String, ProcessEvent> candidateEvents;
        if (topics.candidatePattern() != null) {
            candidateEvents = builder.stream(topics.candidatePattern(), Consumed.with(Serdes.String(), eventSerde));
        } else {
            candidateEvents = builder.stream(topics.candidateTopic(), Consumed.with(Serdes.String(), eventSerde));
        }

        KStream<String, JoinInput> candidateInputs = candidateEvents
                .filter((key, event) -> event != null
                        && isCandidateOutcome(event)
                        && event.processId() != null && event.activityId() != null
                        && event.processInstanceId() != null)
                .map((key, event) -> KeyValue.pair(candidateKey(event), JoinInput.candidate(event)));

        List<String> ignorePaths = topics.ignorePaths();

        KTable<String, ValidationJoinState> joined = priorInputs
                .merge(candidateInputs)
                .groupByKey(Grouped.with(Serdes.String(), joinInputSerde))
                .aggregate(
                        ValidationJoinState::empty,
                        (key, input, state) -> state.merge(input),
                        Materialized.<String, ValidationJoinState, KeyValueStore<Bytes, byte[]>>as(LOCAL_JOIN_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(joinStateSerde)
                );

        joined.toStream()
                .filter((key, state) -> state != null && state.candidate() != null)
                .mapValues(state -> ValidationResult.compare(
                        state.candidate(), state.priorPayload(), state.priorVersion(), ignorePaths))
                .to(topics.resultsTopic(), Produced.with(Serdes.String(), resultSerde));

        builder.globalTable(
                topics.resultsTopic(),
                Consumed.with(Serdes.String(), resultSerde),
                Materialized.<String, ValidationResult, KeyValueStore<Bytes, byte[]>>as(topics.resultsStore())
                        .withKeySerde(Serdes.String())
                        .withValueSerde(resultSerde)
        );

        return builder.build();
    }

    private static String priorKey(ProcessEvent event) {
        return event.processId() + ":" + event.activityId() + ":" + event.processInstanceId();
    }

    private static String candidateKey(ProcessEvent event) {
        return event.processId() + ":" + event.activityId() + ":" + event.processInstanceId();
    }

    /**
     * Whether a validation-shadow event represents a per-task outcome to compare: a successful
     * {@code ACTIVITY_COMPLETED}, or a failure signal (a failed status, an attached error, or an
     * escalation/failure event type) that classifies the candidate as {@code CANDIDATE_ERROR}. The
     * shadow also emits {@code ACTIVITY_ENTERED} and {@code GATEWAY_TAKEN}, which are not outcomes.
     */
    private static boolean isCandidateOutcome(ProcessEvent event) {
        if (event.eventType() == ProcessEvent.EventType.ACTIVITY_COMPLETED) {
            return true;
        }
        return event.status() == ProcessEvent.Status.FAILED
                || event.error() != null
                || event.eventType() == ProcessEvent.EventType.PROCESS_FAILED
                || event.eventType() == ProcessEvent.EventType.ACTIVITY_ESCALATED;
    }

    /**
     * Names, event sources, and comparison ignore paths for the validation topology.
     */
    public record ValidationTopics(
            String candidateTopic,
            Pattern candidatePattern,
            String eventsTopic,
            Pattern eventsPattern,
            String resultsTopic,
            String resultsStore,
            List<String> ignorePaths
    ) {
        /**
         * Matches all production (non-validation) events topics.
         */
        private static final Pattern PRODUCTION_EVENTS_PATTERN =
                Pattern.compile("process-events-(?!.*-validation).*");

        /**
         * Validation topics matching the monitoring app's all-process configuration. Ignore paths
         * are read from the {@code durga.validation.ignore.paths} system property (comma-separated).
         */
        public static ValidationTopics forAllProcesses() {
            return new ValidationTopics(
                    null,
                    Pattern.compile(DEFAULT_CANDIDATE_EVENTS_PATTERN),
                    ProcessMonitoringTopology.DEFAULT_EVENTS_TOPIC,
                    PRODUCTION_EVENTS_PATTERN,
                    DEFAULT_RESULTS_TOPIC,
                    DEFAULT_RESULTS_STORE,
                    ignorePathsFromProperty()
            );
        }

        private static List<String> ignorePathsFromProperty() {
            String raw = System.getProperty("durga.validation.ignore.paths", "");
            List<String> paths = new ArrayList<>();
            for (String part : raw.split(",")) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    paths.add(trimmed);
                }
            }
            return paths;
        }
    }

    /**
     * One side of the merge: either a validation-candidate event or a prior/production event.
     */
    public record JoinInput(ProcessEvent candidate, ProcessEvent prior) {
        static JoinInput candidate(ProcessEvent event) {
            return new JoinInput(event, null);
        }

        static JoinInput prior(ProcessEvent prior) {
            return new JoinInput(null, prior);
        }
    }

    /**
     * Accumulated join state for one {@code processId:activityId:processInstanceId} key.
     */
    public record ValidationJoinState(
            ProcessEvent candidate,
            Map<String, Object> priorPayload,
            String priorVersion
    ) {
        static ValidationJoinState empty() {
            return new ValidationJoinState(null, null, null);
        }

        ValidationJoinState merge(JoinInput input) {
            if (input == null) {
                return this;
            }
            if (input.candidate() != null) {
                return new ValidationJoinState(input.candidate(), priorPayload, priorVersion);
            }
            if (input.prior() != null) {
                return new ValidationJoinState(candidate, input.prior().payload(), input.prior().processVersion());
            }
            return this;
        }
    }
}
