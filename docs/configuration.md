# Configuration reference

Catalog file example: [`config/teradata-export.properties.example`](../config/teradata-export.properties.example).

`connector.name` **must** be `teradata_export`.

## Connection (JDBC control plane)

| Property | Default | Description |
|----------|---------|-------------|
| `teradata.url` | *(required)* | JDBC URL, e.g. `jdbc:teradata://host/DATABASE=DBC,TMODE=TERA` |
| `teradata.user` | *(required)* | Service account used for JDBC |
| `teradata.password` | | Password (prefer `password-script` in production) |
| `teradata.password-script` | | Executable that prints the password to stdout |
| `teradata.timezone` | `-05:00` | Teradata server offset for TIME/TIMESTAMP (`±HH:MM`) |

## Bridge / network (data plane)

| Property | Default | Description |
|----------|---------|-------------|
| `teradata.export.bridge-port` | `9999` | TCP listen port for the binary bridge on each worker |
| `teradata.export.trino-address` | `localhost` | Single-worker host advertised to Teradata |
| `teradata.export.worker-advertised-addresses` | | Comma-separated `host:port` list reachable **from AMPs** (multi-worker / NAT) |
| `teradata.export.flight-port` | `50051` | Legacy Flight bind (not used on the hot path) |
| `teradata.export.flight-bind-address` | `0.0.0.0` | Legacy Flight bind address |

## Performance

| Property | Default | Description |
|----------|---------|-------------|
| `teradata.export.batch-size` | `500000` | Rows per compressed batch from the UDF |
| `teradata.export.socket-receive-buffer-size` | `134217728` | SO_RCVBUF (bytes) |
| `teradata.export.input-buffer-size` | `16777216` | Application read buffer |
| `teradata.export.buffer-queue-capacity` | `500` | Max buffered pages per query on a worker |
| `teradata.export.page-poll-timeout-ms` | `100` | PageSource poll interval |
| `teradata.export.splits-per-worker` | `8` | Parallel splits scheduled per worker |
| `teradata.export.compression-enabled` | `true` | Enable batch compression |
| `teradata.export.compression-algorithm` | `ZLIB`* | `ZLIB` or `LZ4` (*example config may set LZ4) |
| `teradata.export.max-bridge-threads` | | Max bridge handler threads |
| `teradata.export.bridge-core-pool-size` | | Core pool size for bridge executors |
| `teradata.export.bridge-queue-capacity` | | Work queue for bridge tasks |
| `teradata.export.max-query-concurrency` | | Soft concurrency guard |

\* Defaults come from `TrinoExportConfig`; always confirm against code when tuning.

## Pushdown & filters

| Property | Default | Description |
|----------|---------|-------------|
| `teradata.export.enable-dynamic-filtering` | `true` | Dynamic filter support |
| `teradata.export.dynamic-filter-timeout` | `10s` | Wait for dynamic filters |
| `teradata.export.enable-aggregation-pushdown` | `true` | COUNT/SUM/MIN/MAX/AVG |
| `teradata.export.enable-topn-pushdown` | `true` | ORDER BY + LIMIT / SAMPLE |
| `teradata.export.enable-join-pushdown` | `true` | Join pushdown |
| `teradata.export.enable-complex-join-pushdown` | `true` | Multi-way / complex joins |
| `teradata.export.enable-complex-expression-pushdown` | `true` | Complex predicate rewrite |
| `teradata.export.domain-compaction-threshold` | `100` | Predicate domain compaction |

## Security

| Property | Default | Description |
|----------|---------|-------------|
| `teradata.export.enforce-proxy-authentication` | `true` | Fail if `PROXYUSER` cannot be applied |

Per-query data-plane tokens are generated automatically (static catalog tokens are obsolete).

## UDF location

| Property | Default | Description |
|----------|---------|-------------|
| `teradata.export.udf-database` | `TrinoExport` | Database hosting the table operator |
| `teradata.export.udf-name` | `ExportToTrino` | Function name |

## Metadata

| Property | Default | Description |
|----------|---------|-------------|
| `teradata.export.default-schemas` | `TrinoExport,default` | Schemas of interest |
| `teradata.export.metadata-cache-size` | | Planner cache size |
| `teradata.export.metadata-refresh-interval` | | Background refresh period (if enabled in config) |
| `teradata.export.metadata-refresh-schemas` | | Schemas warmed by `MetadataRefreshService` |

## Session / system query

| Property | Default | Description |
|----------|---------|-------------|
| `teradata.export.system-query-enabled` | `true` | Enable `system_query` table function |
| `teradata.export.system-query-max-length` | `1048576` | Max remote SQL length |
| `teradata.export.enable-debug-logging` | `false` | Verbose connector logging |

## Multi-worker example

```properties
connector.name=teradata_export
teradata.url=jdbc:teradata://td.example.com/CHARSET=UTF8,TMODE=TERA
teradata.user=trino_svc
teradata.password-script=/etc/trino/secrets/td.sh
teradata.timezone=+00:00
teradata.export.bridge-port=9999
teradata.export.worker-advertised-addresses=10.0.1.11:9999,10.0.1.12:9999
teradata.export.compression-algorithm=LZ4
teradata.export.udf-database=TrinoExport
teradata.export.enforce-proxy-authentication=true
```

Ensure every worker process listens on the advertised bridge port (or map ports consistently).
