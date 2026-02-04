# Flink Consul HA

Consul-based High Availability backend for Apache Flink 2.1 and 2.2.

## Requirements

- Java 17
- Apache Flink 2.1.x or 2.2.x
- Consul agent (for leader election and KV storage)

## Build

Build for **Flink 2.2** (default):

```bash
mvn clean package
```

Build for **Flink 2.1**:

```bash
mvn clean package -Dflink.version=2.1
```

The JAR will be in `target/flink-consul-ha-1.0.0-SNAPSHOT.jar`. Place it in Flink's `lib/` directory (or use as a plugin).

## Configuration

Set the following in `conf/flink-conf.yaml` (or equivalent) to use Consul HA:

```yaml
# Use the Consul HA factory (required)
high-availability: com.fredwangwang.flink.consul.ha.ConsulHaServicesFactory

# Standard HA options (same as ZooKeeper HA)
high-availability.storageDir: hdfs:///flink/ha
high-availability.cluster-id: /default

# Consul-specific options
high-availability.consul.host: localhost
high-availability.consul.port: 8500
high-availability.consul.path.root: flink
high-availability.consul.path.leader: leader
high-availability.consul.path.execution-plans: execution-plans
high-availability.consul.path.jobs: jobs

# Optional: session and blocking query timeouts
high-availability.consul.session-ttl: 30s
high-availability.consul.blocking-query-wait: 30s
```

### Authentication

ACL token authentication is supported via the `high-availability.consul.acl-token` configuration option. The implementation uses the [Vert.x Consul Client](https://vertx.io/docs/vertx-consul-client/java/), which is actively maintained and supports the `aclToken` option on the client.

## Options (aligned with ZooKeeper HA)

| Option | Default | Description |
|--------|---------|-------------|
| `high-availability.consul.host` | localhost | Consul agent host |
| `high-availability.consul.port` | 8500 | Consul HTTP port |
| `high-availability.consul.path.root` | flink | Root path in Consul KV |
| `high-availability.consul.path.leader` | leader | Path segment for leader election |
| `high-availability.consul.path.execution-plans` | execution-plans | Path for execution plans |
| `high-availability.consul.path.jobs` | jobs | Path for job data (checkpoints, etc.) |
| `high-availability.consul.acl-token` | (none) | Consul ACL token for authenticated requests |
| `high-availability.consul.session-ttl` | 30s | Session TTL for leader latch |
| `high-availability.consul.blocking-query-wait` | 30s | Blocking query wait for leader retrieval |

Standard Flink HA options (`high-availability.storageDir`, `high-availability.cluster-id`) are used as with ZooKeeper HA.

## Layout in Consul KV

- `{root}/{cluster-id}/leader/latch` – leader election latch (session-bound)
- `{root}/{cluster-id}/leader/{component}/connection_info` – leader connection info (resource_manager, dispatcher, rest_server, job-&lt;jobId&gt;)
- `{root}/{cluster-id}/execution-plans/{jobId}` – execution plan state handles
- `{root}/{cluster-id}/jobs/{jobId}/checkpoints/...` – completed checkpoint handles
- `{root}/{cluster-id}/jobs/{jobId}/checkpoint_id_counter` – checkpoint ID counter

Actual checkpoint and execution plan state is stored on the file system (HA storage path); Consul holds only pointers (state handles).

## License

Apache License 2.0.
