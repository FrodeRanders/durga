package org.gautelis.durga.monitoring;

import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
public class ProcessMonitoringResource {

    private static final Logger LOG = LoggerFactory.getLogger(ProcessMonitoringResource.class);

    @Inject
    ProcessMonitoringApp.MonitoringState state;

    private final String apiKey = resolveApiKey();

    @GET
    @Path("/health")
    public Response health(@HeaderParam("Authorization") String authorization) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;
        return Response.ok(healthPayload(state)).build();
    }

    @GET
    @Path("/processes/list")
    public Response processList(@HeaderParam("Authorization") String authorization) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;
        try {
            return Response.ok(state.queryService().listProcessIds()).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "state store not queryable yet")).build();
        }
    }

    @GET
    @Path("/instances/{instanceId}")
    public Response instance(@HeaderParam("Authorization") String authorization,
                             @PathParam("instanceId") String instanceId) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;
        try {
            Optional<ProcessStateView> view = state.queryService().findInstance(instanceId);
            if (view.isEmpty()) {
                return Response.status(404).entity(Map.of("error", "not found")).build();
            }
            return Response.ok(view.get()).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "state store not queryable yet")).build();
        }
    }

    @GET
    @Path("/processes/{processId}/counts")
    public Response countsForProcess(@HeaderParam("Authorization") String authorization,
                                     @PathParam("processId") String processId) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;
        try {
            return Response.ok(state.queryService().countsForProcess(processId)).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "state store not queryable yet")).build();
        }
    }

    @GET
    @Path("/processes/{processId}/latency")
    public Response latencyForProcess(@HeaderParam("Authorization") String authorization,
                                      @PathParam("processId") String processId) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;
        try {
            return Response.ok(state.queryService().latencyForProcess(processId)).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "state store not queryable yet")).build();
        }
    }

    @GET
    @Path("/processes/{processId}/trends")
    public Response trendsForProcess(@HeaderParam("Authorization") String authorization,
                                     @PathParam("processId") String processId) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;
        try {
            return Response.ok(state.queryService().trendsForProcess(processId)).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "state store not queryable yet")).build();
        }
    }

    @GET
    @Path("/processes/{processId}/throughput")
    public Response throughputForProcess(@HeaderParam("Authorization") String authorization,
                                         @PathParam("processId") String processId) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;
        try {
            return Response.ok(state.queryService().throughputForProcess(processId)).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "state store not queryable yet")).build();
        }
    }

    @GET
    @Path("/counts")
    public Response allCounts(@HeaderParam("Authorization") String authorization) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;
        try {
            return Response.ok(state.queryService().allCounts()).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "state store not queryable yet")).build();
        }
    }

    @GET
    @Path("/alarms")
    public Response allAlarms(@HeaderParam("Authorization") String authorization) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;
        try {
            return Response.ok(state.alarmQueryService().allAlarms()).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "alarm state store not queryable yet")).build();
        }
    }

    @GET
    @Path("/processes/{processId}/alarms")
    public Response alarmsForProcess(@HeaderParam("Authorization") String authorization,
                                     @PathParam("processId") String processId) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;
        try {
            return Response.ok(state.alarmQueryService().alarmsForProcess(processId)).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "alarm state store not queryable yet")).build();
        }
    }

    @GET
    @Path("/instances/{instanceId}/alarms")
    public Response alarmsForInstance(@HeaderParam("Authorization") String authorization,
                                      @PathParam("instanceId") String instanceId) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;
        try {
            return Response.ok(state.alarmQueryService().alarmsForInstance(instanceId)).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "alarm state store not queryable yet")).build();
        }
    }

    @GET
    @Path("/stuck")
    public Response stuck(
            @HeaderParam("Authorization") String authorization,
            @QueryParam("processId") String processId,
            @QueryParam("olderThanSeconds") @DefaultValue("60") long olderThanSeconds
    ) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;
        try {
            return Response.ok(state.queryService().stuckInstances(processId, olderThanSeconds)).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "state store not queryable yet")).build();
        }
    }

    @GET
    @Path("/validation/results")
    public Response validationResults(
            @HeaderParam("Authorization") String authorization,
            @QueryParam("processId") String processId,
            @QueryParam("taskId") String taskId,
            @QueryParam("status") String status
    ) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;
        try {
            return Response.ok(state.validationQueryService().results(processId, taskId, status)).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "validation store not queryable yet")).build();
        }
    }

    @GET
    @Path("/validation/instances/{instanceId}")
    public Response validationForInstance(@HeaderParam("Authorization") String authorization,
                                          @PathParam("instanceId") String instanceId) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;
        try {
            return Response.ok(state.validationQueryService().resultsForInstance(instanceId)).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "validation store not queryable yet")).build();
        }
    }

    @GET
    @Path("/validation/summary")
    public Response validationSummary(@HeaderParam("Authorization") String authorization,
                                      @QueryParam("processId") String processId) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;
        try {
            var summaries = processId != null && !processId.isBlank()
                    ? state.validationQueryService().summaryForProcess(processId)
                    : state.validationQueryService().allSummaries();
            return Response.ok(summaries).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "validation store not queryable yet")).build();
        }
    }

    @GET
    @Path("/metrics")
    @Produces("text/plain; version=0.0.4")
    public Response metrics() {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("# HELP durga_activity_latency_ms Activity latency in milliseconds\n");
            sb.append("# TYPE durga_activity_latency_ms gauge\n");
            sb.append("# HELP durga_activity_samples Activity sample count\n");
            sb.append("# TYPE durga_activity_samples gauge\n");
            sb.append("# HELP durga_activity_sla_violations SLA threshold violations\n");
            sb.append("# TYPE durga_activity_sla_violations counter\n");

            var allCounts = state.queryService().allCounts();
            for (var count : allCounts) {
                String processId = count.processId();
                if (processId == null || processId.isEmpty()) continue;

                var summaryList = state.queryService().latencyForProcess(processId);
                for (ActivityLatencySummary s : summaryList) {
                    String labels = String.format(
                            "process_id=\"%s\",activity_id=\"%s\"",
                            prometheusLabelValue(s.processId()), prometheusLabelValue(s.activityId()));
                    sb.append(String.format(
                            "durga_activity_latency_ms{pct=\"50\",%s} %d\n", labels, s.p50DurationMs()));
                    sb.append(String.format(
                            "durga_activity_latency_ms{pct=\"95\",%s} %d\n", labels, s.p95DurationMs()));
                    sb.append(String.format(
                            "durga_activity_latency_ms{pct=\"99\",%s} %d\n", labels, s.p99DurationMs()));
                    sb.append(String.format(
                            "durga_activity_latency_ms{pct=\"avg\",%s} %d\n", labels, s.averageDurationMs()));
                    sb.append(String.format(
                            "durga_activity_samples{%s} %d\n", labels, s.sampleCount()));
                    sb.append(String.format(
                            "durga_activity_sla_violations{%s} %d\n", labels, s.slaViolationCount()));
                }
            }
            sb.append("# HELP durga_streams_state Kafka Streams state (1=running)\n");
            sb.append("# TYPE durga_streams_state gauge\n");
            sb.append(String.format("durga_streams_state %d\n",
                    "RUNNING".equals(state.streams().state().name()) ? 1 : 0));

            try {
                var summaries = state.validationQueryService().allSummaries();
                if (!summaries.isEmpty()) {
                    sb.append("# HELP durga_validation_total Validation comparisons per task\n");
                    sb.append("# TYPE durga_validation_total gauge\n");
                    sb.append("# HELP durga_validation_diff Validation comparisons where candidate differs from prior\n");
                    sb.append("# TYPE durga_validation_diff gauge\n");
                    sb.append("# HELP durga_validation_candidate_error Validation comparisons where the candidate errored\n");
                    sb.append("# TYPE durga_validation_candidate_error gauge\n");
                    for (ValidationSummary s : summaries) {
                        String labels = String.format(
                                "process_id=\"%s\",task_id=\"%s\"",
                                prometheusLabelValue(s.processId()), prometheusLabelValue(s.taskId()));
                        sb.append(String.format("durga_validation_total{%s} %d\n", labels, s.total()));
                        sb.append(String.format("durga_validation_diff{%s} %d\n", labels, s.diff()));
                        sb.append(String.format("durga_validation_candidate_error{%s} %d\n", labels, s.candidateError()));
                    }
                }
            } catch (InvalidStateStoreException ignored) {
                // validation store not queryable yet; omit validation metrics
            }

            try {
                var alarms = state.alarmQueryService().allAlarms();
                if (!alarms.isEmpty()) {
                    sb.append("# HELP durga_alarm_active Active alarm states (1 per active alarm)\n");
                    sb.append("# TYPE durga_alarm_active gauge\n");
                    sb.append("# HELP durga_alarm_fire_count Firings folded into an active alarm\n");
                    sb.append("# TYPE durga_alarm_fire_count counter\n");
                    sb.append("# HELP durga_sla_last_observed Latest observed SLA measurement (latency ms, or windowed completion count)\n");
                    sb.append("# TYPE durga_sla_last_observed gauge\n");
                    sb.append("# HELP durga_sla_limit Configured SLA limit (max latency ms, or minimum completions per window)\n");
                    sb.append("# TYPE durga_sla_limit gauge\n");
                    for (AlarmStateView a : alarms) {
                        String activity = a.activityId() != null && !a.activityId().isBlank()
                                ? a.activityId() : "*";
                        String labels = String.format(
                                "process_id=\"%s\",activity_id=\"%s\",syndrome=\"%s\",severity=\"%s\"",
                                prometheusLabelValue(a.processId()), prometheusLabelValue(activity),
                                a.syndrome() != null ? a.syndrome().name() : "",
                                a.severity() != null ? a.severity().name() : "");
                        sb.append(String.format("durga_alarm_active{%s} 1\n", labels));
                        sb.append(String.format("durga_alarm_fire_count{%s} %d\n", labels, a.fireCount()));
                        if (a.syndrome() == AlarmSyndrome.SLA_LATENCY
                                || a.syndrome() == AlarmSyndrome.SLA_THROUGHPUT) {
                            String slaLabels = String.format(
                                    "process_id=\"%s\",activity_id=\"%s\",syndrome=\"%s\"",
                                    prometheusLabelValue(a.processId()), prometheusLabelValue(activity),
                                    a.syndrome().name());
                            sb.append(String.format("durga_sla_last_observed{%s} %d\n", slaLabels, a.lastCount()));
                            sb.append(String.format("durga_sla_limit{%s} %d\n", slaLabels, a.threshold()));
                        }
                    }
                }
            } catch (InvalidStateStoreException ignored) {
                // alarm state store not queryable yet; omit alarm metrics
            }

            return Response.ok(sb.toString()).build();
        } catch (Exception e) {
            return Response.status(503).entity("# error: " + e.getMessage()).build();
        }
    }

    @GET
    @Path("/diagram")
    public Response diagram(@HeaderParam("Authorization") String authorization,
                            @QueryParam("processId") String processId) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;

        // Kafka state store is the canonical source (processes publish models via process-models topic)
        if (processId != null && !processId.isBlank()) {
            try {
                Optional<String> model = state.queryService().findModel(processId);
                if (model.isPresent()) {
                    return Response.ok(model.get(), MediaType.APPLICATION_XML).build();
                }
            } catch (InvalidStateStoreException ignored) {
            }
        }

        return Response.status(404).entity(Map.of("error", "No BPMN diagram available for this process")).build();
    }

    private static String resolveApiKey() {
        String key = System.getProperty("durga.monitoring.api.key");
        if (key == null) {
            key = System.getenv("DURGA_MONITORING_API_KEY");
        }
        if (key != null && key.isBlank()) {
            key = null;
        }
        if (key == null) {
            LOG.warn("No DURGA_MONITORING_API_KEY set — monitoring API is unauthenticated");
        }
        return key;
    }

    private Response requireAuth(String authorization) {
        if (apiKey == null) {
            return null;
        }
        String expected = "Bearer " + apiKey;
        if (authorization == null || !authorization.equals(expected)) {
            return Response.status(401)
                    .header("WWW-Authenticate", "Bearer")
                    .entity(Map.of("error", "unauthorized"))
                    .build();
        }
        return null;
    }

    static Map<String, String> healthPayload(ProcessMonitoringApp.MonitoringState state) {
        return Map.of(
                "streamsState", state.streams().state().name(),
                "faultStreamsState", state.faultStreams().state().name(),
                "alarmStateStreamsState", state.alarmStateStreams().state().name(),
                "validationStreamsState", state.validationStreams().state().name()
        );
    }

    static String prometheusLabelValue(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder escaped = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\' -> escaped.append("\\\\");
                case '"' -> escaped.append("\\\"");
                case '\n' -> escaped.append("\\n");
                default -> escaped.append(c);
            }
        }
        return escaped.toString();
    }
}
