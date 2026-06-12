package org.gautelis.durga.monitoring;

import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.kafka.streams.errors.InvalidStateStoreException;

import java.util.Map;
import java.util.Optional;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
public class ProcessMonitoringResource {

    @Inject
    ProcessMonitoringApp.MonitoringState state;

    @GET
    @Path("/health")
    public Response health() {
        return Response.ok(Map.of(
                "streamsState", state.streams().state().name()
        )).build();
    }

    @GET
    @Path("/instances/{instanceId}")
    public Response instance(@PathParam("instanceId") String instanceId) {
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
    public Response countsForProcess(@PathParam("processId") String processId) {
        try {
            return Response.ok(state.queryService().countsForProcess(processId)).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "state store not queryable yet")).build();
        }
    }

    @GET
    @Path("/processes/{processId}/latency")
    public Response latencyForProcess(@PathParam("processId") String processId) {
        try {
            return Response.ok(state.queryService().latencyForProcess(processId)).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "state store not queryable yet")).build();
        }
    }

    @GET
    @Path("/processes/{processId}/trends")
    public Response trendsForProcess(@PathParam("processId") String processId) {
        try {
            return Response.ok(state.queryService().trendsForProcess(processId)).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "state store not queryable yet")).build();
        }
    }

    @GET
    @Path("/counts")
    public Response allCounts() {
        try {
            return Response.ok(state.queryService().allCounts()).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "state store not queryable yet")).build();
        }
    }

    @GET
    @Path("/stuck")
    public Response stuck(
            @QueryParam("processId") String processId,
            @QueryParam("olderThanSeconds") @DefaultValue("60") long olderThanSeconds
    ) {
        try {
            return Response.ok(state.queryService().stuckInstances(processId, olderThanSeconds)).build();
        } catch (InvalidStateStoreException e) {
            return Response.status(503).entity(Map.of("error", "state store not queryable yet")).build();
        }
    }
}
