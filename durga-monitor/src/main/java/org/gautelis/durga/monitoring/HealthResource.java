package org.gautelis.durga.monitoring;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * Conventional root health endpoint for load balancers and container probes.
 */
@Path("/health")
@Produces(MediaType.APPLICATION_JSON)
public class HealthResource {

    @Inject
    ProcessMonitoringApp.MonitoringState state;

    @GET
    public Response health() {
        return Response.ok(ProcessMonitoringResource.healthPayload(state)).build();
    }
}
