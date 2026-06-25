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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
        return Response.ok(Map.of(
                "streamsState", state.streams().state().name()
        )).build();
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
                            s.processId(), s.activityId());
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

            return Response.ok(sb.toString()).build();
        } catch (Exception e) {
            return Response.status(503).entity("# error: " + e.getMessage()).build();
        }
    }

    @GET
    @Path("/diagram")
    @Produces(MediaType.APPLICATION_XML)
    public Response diagram(@HeaderParam("Authorization") String authorization) {
        Response auth = requireAuth(authorization);
        if (auth != null) return auth;

        java.nio.file.Path path = state.bpmnPath();
        if (path == null) {
            return Response.status(404).entity(Map.of("error", "No BPMN diagram available")).build();
        }

        try {
            String xml = Files.readString(path, StandardCharsets.UTF_8);
            return Response.ok(xml).build();
        } catch (Exception e) {
            LOG.warn("Failed to read BPMN diagram from {}", path, e);
            return Response.status(500).entity(Map.of("error", "Failed to read BPMN diagram")).build();
        }
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
}
