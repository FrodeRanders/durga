package org.gautelis.durga.monitoring;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

/**
 * Minimal CLI wrapper around the monitoring HTTP API.
 */
public final class ProcessMonitoringClient {
    private ProcessMonitoringClient() {
    }

    /**
     * Invokes one monitoring endpoint and prints the raw HTTP status and body.
     *
     * @param args {@code <baseUrl> <command> ...}
     * @throws IOException on transport errors
     * @throws InterruptedException if interrupted while waiting for the response
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        String baseUrl = args.length > 0 ? args[0] : "http://localhost:8081";
        String command = args.length > 1 ? args[1] : "health";

        String path = switch (command) {
            case "health" -> "/health";
            case "instance" -> {
                if (args.length < 3) {
                    throw new IllegalArgumentException("Usage: ProcessMonitoringClient <baseUrl> instance <processInstanceId>");
                }
                yield "/api/instances/" + args[2];
            }
            case "counts" -> {
                if (args.length > 2) {
                    yield "/api/processes/" + args[2] + "/counts";
                }
                yield "/api/counts";
            }
            case "latency" -> {
                if (args.length < 3) {
                    throw new IllegalArgumentException("Usage: ProcessMonitoringClient <baseUrl> latency <processId>");
                }
                yield "/api/processes/" + args[2] + "/latency";
            }
            case "trends" -> {
                if (args.length < 3) {
                    throw new IllegalArgumentException("Usage: ProcessMonitoringClient <baseUrl> trends <processId>");
                }
                yield "/api/processes/" + args[2] + "/trends";
            }
            case "stuck" -> {
                String processId = args.length > 2 ? args[2] : null;
                String olderThanSeconds = args.length > 3 ? args[3] : "60";
                StringBuilder pathBuilder = new StringBuilder("/api/stuck?olderThanSeconds=").append(olderThanSeconds);
                if (processId != null && !processId.isBlank()) {
                    pathBuilder.append("&processId=").append(processId);
                }
                yield pathBuilder.toString();
            }
            default -> throw new IllegalArgumentException("""
                    Usage:
                      ProcessMonitoringClient <baseUrl> health
                      ProcessMonitoringClient <baseUrl> instance <processInstanceId>
                      ProcessMonitoringClient <baseUrl> counts [processId]
                      ProcessMonitoringClient <baseUrl> latency <processId>
                      ProcessMonitoringClient <baseUrl> trends <processId>
                      ProcessMonitoringClient <baseUrl> stuck [processId] [olderThanSeconds]
                    """);
        };

        try (HttpClient client = HttpClient.newHttpClient()) {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(URI.create(normalizeBaseUrl(baseUrl) + path))
                .GET();
            String apiKey = System.getenv("DURGA_MONITORING_API_KEY");
            if (apiKey != null && !apiKey.isBlank()) {
                requestBuilder.header("Authorization", "Bearer " + apiKey);
            }
            HttpRequest request = requestBuilder.build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("HTTP " + response.statusCode());
            System.out.println(response.body());
        }
    }

    private static String normalizeBaseUrl(String baseUrl) {
        if (baseUrl.endsWith("/")) {
            return baseUrl.substring(0, baseUrl.length() - 1);
        }
        return baseUrl;
    }
}
