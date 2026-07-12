# Architecture

This document describes the **current** production architecture of the Trino Teradata
Direct connector. For operator install steps see [installation.md](installation.md).
For SPI packaging and contributor rules see [development.md](development.md).

## Overview

The connector is a **read-oriented Direct path**: JDBC is the control plane
(metadata, planning, pushdown, UDF invocation); bulk row data never returns over
JDBC. Rows stream in parallel from Teradata AMPs to Trino workers over a binary
TCP bridge and are parsed directly into Trino `Page` objects.

```text
┌──────────────────── Trino ────────────────────┐
│  Planner / Optimizer                          │
│       │                                       │
│  TeradataClient + QueryBuilder (JDBC SQL)     │
│       │ wraps SQL in ExportToTrino(...)       │
│  SplitManager ──► workers (local splits)      │
│       │                                       │
│  Bridge Server (per worker JVM)               │
│       │ decompress + DirectTrinoPageParser    │
│  DataBufferRegistry → PageSource → Engine     │
└──────────────────▲────────────────────────────┘
                   │ binary TCP (token auth)
┌──────────────────┴────────────────────────────┐
│  Teradata AMPs                                │
│  ExportToTrino table operator (C UDF)         │
│  serialize → compress (LZ4/ZLIB) → socket     │
└───────────────────────────────────────────────┘
```

## Control plane vs data plane

| Plane | Path | Purpose |
|-------|------|---------|
| **Control** | Trino → Teradata JDBC | Catalog metadata, type mapping, pushdown SQL, `CONNECT THROUGH` / `PROXYUSER`, start table operator, collect routing PIDs for EOS |
| **Data** | Teradata AMP → Trino worker TCP | Bulk row batches (compressed binary) authenticated with a per-query token |

## Query lifecycle

1. **Plan** — Trino assigns the catalog; `TeradataClient` / `TeradataQueryBuilder` push predicates, aggregations, joins, TopN where safe.
2. **Wrap** — Planned SQL is executed on Teradata as input to `ExportToTrino(...)` with bridge addresses, query id, token, and compression settings.
3. **Route** — Each AMP that participates opens a socket to **one** worker chosen by deterministic routing (below).
4. **Stream** — AMP serializes rows into binary batches, compresses, sends; worker decompresses and parses to `Page`.
5. **EOS** — Workers finish only when **expected** AMP connections for that worker complete (deterministic EOS), not by waiting on an arbitrary idle timeout as the primary signal.
6. **Consume** — `TrinoExportPageSource` reads pages from the local `DataBufferRegistry`.

## AMP → worker routing

Each AMP selects a worker with:

```text
routing_id = pid XOR (pid >> 8)     # stable mix of OS PID on the AMP
worker_idx = (unsigned)routing_id % worker_count
```

- Worker addresses are passed as a comma-separated `host:port` list (IPs; `inet_pton`).
- Multi-node: prefer `teradata.export.worker-advertised-addresses` so AMPs reach bridges through NAT/multi-homed networks.
- Single-node: `teradata.export.trino-address` + `teradata.export.bridge-port`.
- Splits set `isRemotelyAccessible = false` so the worker that receives AMP data is the worker that executes the split.

**No row duplication:** each AMP sends its partition to exactly one worker.

## Deterministic end-of-stream (EOS)

Primary path (current design):

1. After the table-operator JDBC phase, the coordinator collects **routing PIDs** returned by the UDF (result column 0 / status stream).
2. For each PID, compute `worker_idx = unsigned(pid) % num_workers` and tally **expected connections per worker**.
3. Coordinator **broadcasts** command `EXPECTED_TERADATA_SIGNALS` (command id `3`) to each worker with that worker’s expected count.
4. Each worker signals EOS when:
   - expected count is known, and
   - received finished connections/signals for that query reach the expected count, and
   - data for those connections is already in the buffer (push-before-decrement).

Deprecated / ignored:

- **`JDBC_FINISHED` (command id `2`)** is accepted by the bridge for backward compatibility but **ignored**. EOS is not driven by JDBC-finished broadcasts.

Fallback:

- If the expected-count broadcast is not received within a long timeout (~60s), the registry may fall back to connection-close heuristics so queries do not hang forever. **This is not the preferred path**; operators should fix routing/advertised addresses if fallbacks appear in logs.

See also [eos.md](eos.md).

## Binary protocol (high level)

Wire format is an internal packed binary schema (not Arrow IPC on the hot path):

- Schema / type metadata for columns in the export stream
- Row batches with null bitmaps and typed payloads
- Optional batch compression: **LZ4** (default in example config) or **ZLIB**
- Control channel on the same bridge for expected-count broadcasts and finished signals

Type codes on the wire use Teradata FNC type numbers (examples):

| Type | Code |
|------|------|
| VARCHAR | 2 |
| BYTEINT | 7 |
| SMALLINT | 8 |
| INTEGER | 9 |
| FLOAT | 10 |
| DECIMAL | 14 |
| DATE | 15 |
| TIME | 16 |
| TIMESTAMP | 17 |
| BIGINT | 36 |

Character UNICODE data is converted **UTF-16LE → UTF-8** in the UDF before send.
TIME/TIMESTAMP binary layouts are decoded in `DirectTrinoPageParser` with
`teradata.timezone` applied.

## Security model

1. **Service account** in catalog config opens JDBC only.
2. **`PROXYUSER`** via `SET QUERY_BAND` maps the Trino end user onto Teradata authorization (`teradata.export.enforce-proxy-authentication=true` recommended).
3. **Per-query security token** is generated for the data plane; AMP connections must present the token.
4. Bridge ports must be reachable only from the Teradata fabric (network policy).

Details: [../SECURITY.md](../SECURITY.md).

## Main components (code map)

| Component | Role |
|-----------|------|
| `TrinoExportPlugin` / `TrinoExportConnectorFactory` | SPI entry; Airlift Bootstrap |
| `TeradataClient` / `TeradataQueryBuilder` | JDBC client, pushdown SQL |
| `TrinoExportSplitManager` / `TrinoExportDynamicFilteringSplitSource` | Splits, UDF execution, EOS expected-count broadcast |
| `TeradataBridgeServer` | Per-worker TCP server |
| `DirectTrinoPageParser` | Binary → Trino `Page` |
| `DataBufferRegistry` | Per-query buffers + deterministic EOS |
| `TrinoExportPageSource` | Engine-facing page consumer |
| `MetadataRefreshService` | Background catalog cache warm-up |
| `teradata-udf/export_to_trino.c` | Table operator serialization + routing |

## What this connector is not

- Not a full bi-directional write connector (INSERT/CTAS to Teradata is not the product focus).
- Not Arrow Flight on the hot path (a legacy Flight server class may still bind for config compatibility; bulk transfer is the binary bridge).
- Independently implemented open-source Direct-style path (table operator + parallel bridge); not an official Trino or Teradata product.
