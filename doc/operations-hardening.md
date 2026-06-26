# Operations Hardening

This guide records the operational assumptions that must be made explicit before
Durga is treated as beta or production-candidate software.

## Kafka Topic Retention

Durga-generated runtimes rely on Kafka both as transport and as durable process
history. Treat retention as part of the process contract.

Recommended defaults:

| Topic category | Cleanup policy | Retention guidance |
| --- | --- | --- |
| Canonical lifecycle event topic | `delete` or long-retention archive policy | Keep long enough for audit, replay, and monitoring recovery. Use infinite retention only when storage growth is actively managed. |
| Process state topic | `compact` | Keep compaction enabled; this topic represents latest state by key. |
| Task input/output topics | `delete` | Size retention for retry windows and operational diagnosis. |
| Monitoring aggregate topics | `compact` where keyed by latest value, otherwise `delete` | Match query-service recovery requirements. |
| Dead-letter topics | `delete` with longer retention | Keep longer than normal task topics so failures can be inspected and replayed. |

Generated topic scripts should be reviewed before production use. Partition
count controls worker parallelism; replication factor controls broker-failure
tolerance.

## State-Store Recovery

The monitoring topology uses Kafka Streams materialized stores. Recovery depends
on changelog topics and the canonical lifecycle stream.

Operational requirements:

- Do not delete Streams changelog topics while the monitoring application is in
  use.
- Keep the canonical lifecycle event topic available for the longest recovery
  window you intend to support.
- Treat the monitoring application ID as persistent infrastructure. Changing it
  creates a fresh Streams application with independent state.
- Back up or archive lifecycle events if regulatory, audit, or replay
  requirements exceed Kafka retention.
- Test topology restart and recovery as part of release verification.

## Security Assumptions

The local and demo configurations use plaintext Kafka listeners for developer
convenience. Production deployments should not inherit that assumption.

Before production use:

- Enable TLS for Kafka client connections when traffic leaves a trusted local
  network.
- Use SASL or mTLS for client authentication.
- Restrict topic access with ACLs by runtime role: generated workers, monitoring
  topology, operators, and replay tooling should not share broad credentials.
- Keep generated helper scripts out of privileged runtime containers unless they
  are required operationally.
- Do not expose the monitoring HTTP API or dashboard without network controls or
  authentication in shared environments.
- Set `DURGA_MONITORING_API_KEY` or `-Ddurga.monitoring.api.key=<key>` to require
  `Authorization: Bearer <key>` on monitoring JSON endpoints. Keep `/api/metrics`
  behind the same network controls when it is scraped without an API token.
- Avoid logging full payloads when payloads may contain personal, regulated, or
  customer-sensitive data.

## Upgrade Compatibility

Durga has two compatibility boundaries: generated source code and Kafka event
contracts. Both need explicit handling.

For generated code:

- Regenerate into a clean branch and review generated diffs before replacing
  existing workers.
- Preserve hand-written business logic and custom activity implementations.
- Treat generated topic names, consumer group IDs, and package names as stable
  deployment identifiers unless a migration is planned.

For Kafka contracts:

- Keep `ProcessEvent` JSON fields backward compatible across minor releases.
- Add new event fields as optional fields first.
- Do not rename lifecycle states or activity identifiers without a migration
  plan for monitoring and downstream consumers.
- Run both unit and Docker-backed integration tests before upgrading a deployed
  process.

## Release Verification

A release candidate should pass these gates:

```bash
cd monitoring-ui && npm ci && npm test && npm run build
cd ..
mvn test -Dtest='!*IntegrationTest'
mvn test -Dtest='*IntegrationTest' -Ddurga.integration.requireDocker=true
```

Use the container fallback in `setup/run-integration-tests.sh` if the host
cannot expose Docker to Testcontainers directly.

## Open Operational Work

The following items remain maturity work:

1. Generate production-ready topic manifests for operator-managed Kafka
   environments.
2. Add documented ACL examples for the standard topic layout.
3. Add a monitoring backup/replay procedure.
4. Define versioned compatibility guarantees for `ProcessEvent` and generated
   project layouts.
5. Add a release checklist that records integration-test evidence and supported
   Kafka/JDK versions.
