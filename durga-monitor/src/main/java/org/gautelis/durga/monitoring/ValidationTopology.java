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
import org.gautelis.durga.validation.ValidationCandidateOutput;
import org.gautelis.durga.validation.ValidationResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Kafka Streams read model for validation mode.
 * <p>
 * Pairs a shadow worker's candidate output (from {@code validation-candidate-outputs}) against the
 * prior/production output for the same input — the {@code ACTIVITY_COMPLETED} lifecycle event on
 * the process-events stream — and computes a {@link ValidationResult} per task per instance.
 * <p>
 * The two sides are keyed identically on {@code processId:activityId:processInstanceId} and merged
 * into a single aggregate so the comparison is robust to arrival order: in live-concurrent mode the
 * candidate may arrive before or after the production output. Whichever arrives first seeds the
 * join state; when the candidate is present a result is emitted (a candidate seen before any prior
 * output yields {@code PRIOR_MISSING}, which is superseded when the prior output later arrives).
 */
public final class ValidationTopology {

    public static final String DEFAULT_CANDIDATE_TOPIC = "validation-candidate-outputs";
    public static final String DEFAULT_RESULTS_TOPIC = "validation-results";
    public static final String DEFAULT_RESULTS_STORE = "validation-results-store";

    private static final String LOCAL_JOIN_STORE = "validation-join-store";

    private ValidationTopology() {
    }

    /**
     * Builds the validation comparison topology.
     *
     * @param topics topic, store, and ignore-path configuration
     * @return topology that joins candidate and prior output and materializes comparison results
     */
    public static Topology buildTopology(ValidationTopics topics) {
        StreamsBuilder builder = new StreamsBuilder();
        var eventSerde = JsonSerde.forClass(ProcessEvent.class);
        var candidateSerde = JsonSerde.forClass(ValidationCandidateOutput.class);
        var joinInputSerde = JsonSerde.forClass(JoinInput.class);
        var joinStateSerde = JsonSerde.forClass(ValidationJoinState.class);
        var resultSerde = JsonSerde.forClass(ValidationResult.class);

        KStream<String, ProcessEvent> events;
        if (topics.eventsPattern() != null) {
            events = builder.stream(topics.eventsPattern(), Consumed.with(Serdes.String(), eventSerde));
        } else {
            events = builder.stream(topics.eventsTopic(), Consumed.with(Serdes.String(), eventSerde));
        }

        KStream<String, JoinInput> priorInputs = events
                .filter((key, event) -> event != null
                        && event.eventType() == ProcessEvent.EventType.ACTIVITY_COMPLETED
                        && event.processId() != null && event.activityId() != null
                        && event.processInstanceId() != null)
                .map((key, event) -> KeyValue.pair(priorKey(event), JoinInput.prior(event)));

        KStream<String, JoinInput> candidateInputs = builder
                .stream(topics.candidateTopic(), Consumed.with(Serdes.String(), candidateSerde))
                .filter((key, candidate) -> candidate != null
                        && candidate.processId() != null && candidate.taskId() != null
                        && candidate.processInstanceId() != null)
                .map((key, candidate) -> KeyValue.pair(candidate.key(), JoinInput.candidate(candidate)));

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
        return ValidationCandidateOutput.key(event.processId(), event.activityId(), event.processInstanceId());
    }

    /**
     * Names, event source, and comparison ignore paths for the validation topology.
     */
    public record ValidationTopics(
            String candidateTopic,
            String eventsTopic,
            Pattern eventsPattern,
            String resultsTopic,
            String resultsStore,
            List<String> ignorePaths
    ) {
        private static final Pattern ALL_EVENTS_PATTERN = Pattern.compile("process-events-.*");

        /**
         * Validation topics matching the monitoring app's all-process configuration. Ignore paths
         * are read from the {@code durga.validation.ignore.paths} system property (comma-separated).
         */
        public static ValidationTopics forAllProcesses() {
            return new ValidationTopics(
                    DEFAULT_CANDIDATE_TOPIC,
                    ProcessMonitoringTopology.DEFAULT_EVENTS_TOPIC,
                    ALL_EVENTS_PATTERN,
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
     * One side of the merge: either a candidate output or a prior/production event.
     */
    public record JoinInput(ValidationCandidateOutput candidate, ProcessEvent prior) {
        static JoinInput candidate(ValidationCandidateOutput candidate) {
            return new JoinInput(candidate, null);
        }

        static JoinInput prior(ProcessEvent prior) {
            return new JoinInput(null, prior);
        }
    }

    /**
     * Accumulated join state for one {@code processId:activityId:processInstanceId} key.
     */
    public record ValidationJoinState(
            ValidationCandidateOutput candidate,
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
