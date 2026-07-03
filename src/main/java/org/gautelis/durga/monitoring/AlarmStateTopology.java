package org.gautelis.durga.monitoring;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Kafka Streams read model for alarm state.
 * <p>
 * Consumes {@code fault-alarms} and folds repeated alarm events into one
 * {@link AlarmStateView} per process/activity/config scope.
 */
public final class AlarmStateTopology {
    public static final String DEFAULT_ALARM_STATE_STORE = "alarm-state-store";

    private AlarmStateTopology() {
    }

    public static Topology buildTopology(String alarmsTopic, String alarmStateStore) {
        StreamsBuilder builder = new StreamsBuilder();
        var alarmEventSerde = JsonSerde.forClass(AlarmEvent.class);
        var alarmStateSerde = JsonSerde.forClass(AlarmStateView.class);

        builder.stream(alarmsTopic, Consumed.with(Serdes.String(), alarmEventSerde))
                .filter((key, event) -> event != null)
                .selectKey((key, event) -> AlarmStateView.keyFor(event))
                .mapValues(AlarmStateView::fromEvent)
                .groupByKey(Grouped.with(Serdes.String(), alarmStateSerde))
                .reduce(
                        AlarmStateView::merge,
                        Materialized.<String, AlarmStateView, KeyValueStore<Bytes, byte[]>>as(alarmStateStore)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(alarmStateSerde)
                );

        return builder.build();
    }
}
