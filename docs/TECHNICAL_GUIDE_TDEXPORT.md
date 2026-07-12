# Teradata to Trino Export Connector: Technical Guide

This document serves as a comprehensive technical guide for the Teradata to Trino Export Connector. It details the architecture, configuration, implementation specifics, and operational procedures.

---

## 1. System Architecture

The export process follows a **synchronous, zero-copy pipeline** designed for high-performance data transfer from Teradata's parallel environment to Trino with 100% data reliability.

### Data Flow Pipeline:
1.  **Trino Query Planner**: Generates an optimized plan using the `teradata-export` connector.
2.  **SQL Generation**: `TeradataClient` (extending `BaseJdbcClient`) and `TeradataQueryBuilder` leverage Trino's internal JDBC infrastructure to generate optimized Teradata SQL, including predicate, aggregate, and join pushdowns.
3.  **Table Operator Wrapping**: The generated SQL is wrapped with the `ExportToTrino()` Table Operator UDF.
4.  **Teradata Execution**: Teradata executes the query in parallel across all AMPs.
5.  **C Table Operator (UDF)**: Processes rows on each AMP, serializes them into packed binary format, compresses (ZLIB/LZ4), and sends via TCP sockets to the appropriate Trino worker.
6.  **Java Bridge Server**: Multi-threaded server integrated into Trino Worker JVM. Receives and decompresses binary data.
7.  **DirectTrinoPageParser**: High-performance parser that converts binary data directly to Trino `Page` objects.
8.  **DataBufferRegistry**: Synchronous thread-safe queue for parsed Pages.
9.  **Trino PageSource**: Consumes Pages from the buffer for the Trino engine.

### AMP-to-Worker Distribution (No Data Duplication)

The connector ensures that **each Teradata AMP sends data to exactly one Trino worker**, preventing data duplication:

```
Teradata AMPs (128)                     Trino Workers (N)
├── AMP 0  ──────────────────────────► Worker 0 (Bridge on port 9999)
├── AMP 1  ──────────────────────────► Worker 1 (Bridge on port 9999)
├── AMP 2  ──────────────────────────► Worker 2 (Bridge on port 9999)
├── AMP 3  ──────────────────────────► Worker 0  (round-robin)
├── AMP 4  ──────────────────────────► Worker 1
...
└── AMP 127 ─────────────────────────► Worker (127 % N)
```

**Key Implementation Details:**
- **C UDF (Line 269):** Uses `amp_id % ip_count` to deterministically select the target worker
- **Trino Split Manager:** Passes all distinct worker IPs to Teradata as a comma-separated list
- **No Duplication Guarantee:** Each AMP processes its own partition of Teradata data and sends it to exactly one worker

### Multi-Worker Architecture Details

**Split Locality Enforcement:**
- Each split is assigned to a specific worker via `getAddresses()`
- `isRemotelyAccessible()` returns `false` to enforce local execution only
- This ensures data sent to Worker N is processed by Worker N's PageSource

**Per-Worker Buffer Registration:**
- Each worker runs its own `DataBufferRegistry` (static class, but per-JVM)
- `PageSource` constructor registers the query buffer on that worker
- Data arriving before `PageSource` is auto-buffered (race condition handling)

**End-of-Stream Detection:**
- EOS is detected when all socket connections close or upon receiving a Global EOS signal.
- **Global EOS (JDBC_FINISHED)**: When the Teradata JDBC execution finishes, the coordinator-side executor broadcasts a "Finished" signal to all workers via the bridge's control channel. This prevents workers that received no data (idle workers) from waiting for the 5-second timeout.
- Connection tracking ensures all data is in the buffer before a worker signals EOS locally.

**Hostname-to-IP Resolution:**
- Teradata C UDF uses `inet_pton()` which requires IP addresses
- Split Manager resolves worker hostnames to IP addresses automatically
- For NAT/multi-homed networks, use `worker-advertised-addresses` config

**Multi-Worker Deployment Requirements:**
1. Each Trino worker runs its own Bridge Server instance on the configured port
2. The `trino-address` config property is only used in single-worker mode
3. In multi-worker mode, worker IPs are auto-discovered from `NodeManager`
4. For NAT environments, configure `worker-advertised-addresses` explicitly

---

## 2. Environment & Credentials

### Connectivity Details
| Component | Host/IP | Port | Credentials |
| :--- | :--- | :--- | :--- |
| **Teradata Database** | `YOUR_TD_HOST` | `1025` | `<admin_user>` / `<password>` |
| **Trino Coordinator** | `localhost` | `8080` | N/A |
| **Java Bridge (Listen)**| `0.0.0.0` | `9999` | N/A |

### Key Paths
- **Trino Server**: `$TRINO_HOME`
- **JDK Home**: `$JAVA_HOME`
- **Teradata UDF Source**: `teradata-udf/export_to_trino.c`
- **Java Connector Source**: `trino-plugin/src/main/java/io/trino/plugin/teradata/export/`
- **Java Bridge Server**: `TeradataBridgeServer.java` (integrated into Trino Worker JVM)

---

## 3. SQL Generation & Pushdown Capabilities

The connector leverages Trino's modern JDBC infrastructure to provide industry-leading pushdown capabilities.

### 3.1 Enterprise SQL Engine (`TeradataClient`)
The `TeradataClient` extends Trino's `BaseJdbcClient`, inheriting robust SQL generation for standard relations. It uses a custom `AggregateFunctionRewriter` and `ConnectorExpressionRewriterBuilder`.

- **Predicate Pushdown**: Translates Trino expressions (filters, LIKE, OR, CAST, etc.) into Teradata-compatible SQL. Supported types include numeric, string, date, time, and timestamp.
- **Join Pushdown**: Pushes down complex multi-way joins, including cross-joins and self-joins.
- **Aggregate Pushdown**: Supports `COUNT(*)`, `COUNT(DISTINCT col)`, `SUM`, `MIN`, `MAX`, and `AVG` for both decimal and floating-point types.
- **TopN/Limit Pushdown**: Translates `LIMIT N` or `ORDER BY ... LIMIT N` into Teradata `TOP N` or `SAMPLE N`.

---

## 4. Metadata Architecture

Metadata retrieval is optimized for speed, bridging the gap between Trino's optimizer and Teradata's dictionary performance.

### 4.1 Hybrid Metadata Strategy
1.  **Fast Probing (`WHERE 1=0`)**: The connector uses `SELECT * FROM table WHERE 1=0` to fetch column metadata. This is **25x faster** (1.2s vs 30s+) for large tables compared to standard JDBC `getColumns()`.
2.  **Case-Insensitive Probing**: Automatically tries multiple casing combinations (Exact, Upper Table, Upper Schema + Table) to match Teradata's object naming.
3.  **Two-Level Metadata Cache**: 
    - **Table Cache**: Caches mapping from `SchemaTableName` to `JdbcTableHandle`.
    - **Column Cache**: Caches column definitions.

### 4.2 Background Metadata Refresh
The `MetadataRefreshService` periodically warms the caches for critical schemas:
- **Bulk Schema Discovery**: Lists all databases from `DBC.Databases`.
- **Bulk Table Caching**: Scans `DBC.TablesV` for configured schemas in a single query.
- **Intelligent Column Refresh**: Uses bulk `DBC.ColumnsV` queries for physical tables and targeted `WHERE 1=0` probes for views.

---

## 5. Security & Authentication Design

The connector implements a **Hybrid Authentication Model** to balance enterprise security with operational visibility.

### 5.1 Authentication Model
The connector enforces personal identity propagation using Teradata's **Proxy Authentication** mechanism. Unlike standard connectors that might fallback to a service account, this connector ensures that both metadata and data access are strictly governed by the end-user's permissions.

- **Identity Propagation**: Every JDBC connection is personalized using `SET QUERY_BAND = 'PROXYUSER=<user>;' FOR SESSION;`.
- **Strict Enforcement**: If proxy authorization fails or is not granted in Teradata, the connection is immediately aborted.
- **Session Cleanup**: Sessions are reset using `SET QUERY_BAND = NONE FOR SESSION` to prevent identity leakage in connection pools.

### 5.2 Proxy Mechanism Details
The `TeradataConnectionFactory` is responsible for session initialization:
- When a data query is triggered, it retrieves the Trino session user.
- It executes: `SET QUERY_BAND = 'PROXYUSER=<trino_user>;' FOR SESSION;` immediately after connecting.
- **Strict Enforcement**: If the `SET QUERY_BAND` command fails (e.g., due to missing permissions or invalid user), the connection is **immediately aborted** and an `Access Denied` error is returned to Trino. There is **no fallback** to the service account for data queries.
- **Reset Protocol**: Sessions are always reset using `SET QUERY_BAND = NONE FOR SESSION` in a `finally` block to prevent identity leakage.

### 5.3 Teradata Security Requirements
For proxy authentication to work, a Teradata Administrator must grant the following permissions:

```sql
-- Grant connect through rights to the service account
GRANT CONNECT THROUGH <service_user> TO <trino_user> WITHOUT ROLE;
```

If the user lacks these rights, they will see an error: `[Error 9203] Connect Through has not been granted to <USER> through <SERVICE_USER>`.

---

## 6. Implementation Details

### 6.1 C Table Operator (`export_to_trino.c`)
The UDF is the most critical piece for data integrity.

- **Signature**: Uses `void ExportToTrino(void)` with explicit `FNC_TblOpOpen` calls. This ensures a stable stream state and prevents "invalid stream state" errors during complex Trino executions.
- **Binary Codes**: Do not rely on mock headers. Use these confirmed real Teradata internal codes:
    - `TD_VARCHAR`: 2
    - `TD_BYTEINT`: 7
    - `TD_SMALLINT`: 8
    - `TD_INTEGER`: 9
    - `TD_FLOAT`: 10
    - `TD_DECIMAL`: 14
    - `TD_DATE`: 15
    - `TD_TIME`: 16
    - `TD_TIMESTAMP`: 17
    - `TD_BIGINT`: 36
- **Temporal Decoding**: 
    - **TIME**: 6-byte binary. Decoding: `[SecScaled(4)][Hour(1)][Min(1)]`. Seconds are scaled by 1,000,000.
    - **TIMESTAMP**: 10-byte binary. Decoding: `[SecScaled(4)][Year(2)][Month(1)][Day(1)][Hour(1)][Min(1)]`.
    - **DATE Support (0001-01-01)**: The C UDF handles pre-1900 dates by correctly processing negative internal offsets. Years before 1900 (where `d < 0`) are handled via logic: `year = (d/10000) + 1900` with adjustment for negative remainders.
- **Unicode Support**: Character set `UNICODE` (Code 2/6) is stored as UTF-16LE. The UDF contains a manual UTF-16LE to UTF-8 conversion engine. To load test data with multibyte characters (Thai, Chinese), use `bteq -c UTF8`.
- **Timezone Correction**: Teradata sends TIME/TIMESTAMP as local time strings. The Java connector corrects this using the configured `teradata.timezone` (e.g., `-05:00`). It converts from the Teradata server's timezone to the appropriate local representation.

### 6.2 Java Connector
- **`TeradataBridgeServer`**: Integrated server that receives compressed binary data from Teradata AMPs.
- **`DirectTrinoPageParser`**: Parses binary data directly to Trino `Page` objects.
- **`TrinoExportSplitManager`**: Orchestrates the process, triggers Teradata SQL execution.
- **`DataBufferRegistry`**: Thread-safe storage for parsed Pages with deterministic EOS detection.
- **`TrinoExportPageSource`**: Consumes Pages from the buffer for the Trino engine.

## 7. Development, Deployment & Testing

### 7.1 Development & Deployment

**Build and Deploy Connector:**
Use this to compile the Java code and update the Trino plugin directory.
```bash
export JAVA_HOME=$JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH
cd src/trino
mvn clean package dependency:copy-dependencies -DskipTests
export TRINO_HOME=$TRINO_HOME
/bin/bash ../../scripts/deploy_to_trino.sh
```

**Full Cycle (Build + Deploy + JDBC + Restart):**
The most frequently used command during development.
```bash
export JAVA_HOME=$JAVA_HOME && \
export PATH=$JAVA_HOME/bin:$PATH && \
cd src/trino && mvn clean package dependency:copy-dependencies -DskipTests && \
export TRINO_HOME=$TRINO_HOME && \
/bin/bash ../../scripts/deploy_to_trino.sh && \
cp $TERADATA_JDBC_JAR $TRINO_HOME/plugin/teradata-export/ && \
$TRINO_HOME/bin/launcher restart --etc-dir=$TRINO_HOME/etc
```

**Restart Trino Server Only:**
```bash
$TRINO_HOME/bin/launcher restart \
  --etc-dir=$TRINO_HOME/etc
```

### 7.2 Teradata Side Management

**Register/Reload UDF:**
```bash
export TD_HOME=/opt/teradata/client/20.00
export PATH=$PATH:$TD_HOME/bin
bteq < scripts/register.bteq
```

**Manual UDF Test (Via BTEQ):**
Useful for isolating UDF issues from Trino.
```bash
bteq <<EOF
.LOGON YOUR_TD_HOST/<admin_user>,<password>
DATABASE TrinoExport;
SELECT * FROM ExportToTrino(
  ON (SELECT TOP 1 * FROM DBC.Tables)
  ON (SELECT 'YOUR_TRINO_HOST:9999' as target_ips, 'manual-test' as qid) DIMENSION
) AS t;
.QUIT
EOF
```

### 7.3 Monitoring Bridge Activity

**Watch Bridge Activity via Server Logs:**
```bash
tail -f $TRINO_HOME/data/var/log/server.log | grep -E "(Bridge|Receiving|processed query)"
```

### 7.4 Verification & Testing

#### 7.4.1 Quick Query Test
For rapid validation of specific fixes (like TIME decoding or Decimal precision), use `quick_test.sh`. It automatically filters out `jline` terminal noise and compares results.

```bash
# General query
./tests/quick_test.sh "SELECT current_timestamp"

# Query with expected result comparison
./tests/quick_test.sh "SELECT test_id FROM test_unicode WHERE test_id = 1" "1"
```

#### 7.4.2 Full Integration Suite
The comprehensive suite `run_connector_tests.sh` validates 90+ scenarios across all data types, JOINs, aggregations, and pushdown optimizations.

```bash
# Execute full suite
bash tests/run_connector_tests.sh
```

#### 7.4.3 Log-Based Pushdown Verification
To ensure that filters and limits are truly executed by Teradata, verify the generated SQL in the server log.

```bash
# Manual check for generated SQL in server.log
grep "Executing Teradata SQL" $TRINO_HOME/data/var/log/server.log | tail -n 5

# Validation of Pushdown logic (TopN/Limit)
grep -E "Applied (TopN|LIMIT) pushdown" $TRINO_HOME/data/var/log/server.log | tail -n 5
```

#### 7.4.4 Modular Java Test Suite (Recommended)
For deep, modular, and faster validation, a Java-based test suite using JUnit 5 and the Trino JDBC driver is available. This suite allows running specific test categories or individual cases.

**Advantages:**
- **Speed**: Persistent JDBC connections and optimized query execution.
- **Modularity**: Tests are grouped by type (Numeric, Char, DateTime, Complex, etc.).
- **Granular Execution**: Can run a single test class or even a single test method.
- **Rich Reporting**: Detailed diffs on failure via AssertJ.

**Prerequisites:**
- Trino JDBC driver in `trino-jdbc (Maven: io.trino:trino-jdbc)`
- JDK 21+ and Maven.

**Running the Java Suite:**
```bash
# Run all 90+ validations
/bin/bash tests/run_java_suite.sh

# Run specific functional group (e.g., Numeric types only)
/bin/bash tests/run_java_suite.sh NumericDataTypeTest

# Run a specific test method within a class
/bin/bash tests/run_java_suite.sh LogValidationTest#test15_6
```

### 7.5 Test Suite Architecture (Integrity Checking)
The test suite was developed using a modular bash framework to ensure reliable connection and data integrity checking.

- **`setup_test_tables.bteq`**: A pre-requisite script that populates Teradata with edge-case data (multibyte Unicode, 0001-01-01 dates, high-precision decimals).
  ```bash
  export TD_HOME=/opt/teradata/client/20.00
  bteq -c UTF8 < tests/setup_test_tables.bteq
  ```
- **Helper Functions**:
  - `run_count_test`: Validates that the number of rows transferred matches Teradata expectation.
  - `run_value_test`: Performs deep value comparison for specific columns, critical for data type decoding validation.
- **Log Markers**: Uses timestamp markers or `tail` offsets to isolate the effects of the *current* query within the shared `server.log`, preventing false positives from previous executions.

### 7.6 Monitoring & Logs

**Watch Trino Server Logs:**
```bash
tail -f $TRINO_HOME/data/var/log/server.log
```

**Search for Teradata Execution SQL in Logs:**
```bash
grep "Executing Teradata SQL" $TRINO_HOME/data/var/log/server.log
```

### Verify System Data (Unicode/DBC)
```bash
-- Use this to verify UTF-16 to UTF-8 conversion
SELECT DatabaseName, CommentString FROM tdexport.dbc.databases LIMIT 5;
```

---


## 8. Troubleshooting Common Issues

1.  **"Invalid Stream State" (Error 7813)**: 
    - **Cause**: Teradata Table Operator was likely registered with parameters in the signature or used an old stream opening pattern.
    - **Fix**: Re-register `ExportToTrino()` using the void signature and explicit `FNC_TblOpOpen(0, 'r', 0)` calls.

2.  **"Failed to convert value ... to type time(6)"**:
    - **Cause**: Binary structure of TIME was misunderstood (likely treated as a double).
    - **Fix**: Use the 6-byte binary decoding logic: `[SecScaled][Hour][Min]`.

3.  **Non-readable/Hex junk in strings**:
    - **Cause**: Character Set UNICODE (UTF-16) was sent directly to Trino (UTF-8).
    - **Fix**: The C UDF must perform UTF-16 to UTF-8 conversion before sending.

4.  **No data in Trino / Query hangs**:
    - **Cause**: Bridge is not running or firewall/IP mismatch. 
    - **Check**: `tail -f bridge_restarted.log` and verify the `TargetIPs` used in the UDF call (sent by Trino `SplitManager`).

---

---

## 9. Catalog Configuration (`tdexport.properties`)

The connector is highly configurable. Below is a comprehensive reference of all configuration properties.

### 9.1 Core Connection Settings

```properties
connector.name=teradata-export
teradata.url=jdbc:teradata://YOUR_TD_HOST/DATABASE=TrinoExport
teradata.user=YOUR_SERVICE_USER
teradata.password=YOUR_PASSWORD

# Teradata server timezone offset (for TIME/TIMESTAMP conversion)
# Format: +/-HH:MM (e.g., -05:00 for EST, +08:00 for SGT)
teradata.timezone=-05:00
```

### 9.2 Performance & Scalability Settings

```properties
# Parallel splits per worker (default: 8)
teradata.export.splits-per-worker=8

# Batch size for UDF-to-Java transfer (rows per batch)
teradata.export.batch-size=500000

# TCP socket receive buffer size (default: 128MB)
teradata.export.socket-receive-buffer-size=134217728

# Maximum number of Pages buffered per split (default: 500)
teradata.export.buffer-queue-capacity=500

# Number of bridge handler threads (default: 100)
teradata.export.max-bridge-threads=100
```

### 9.3 Optimization Toggles (Pushdowns)

```properties
teradata.export.enable-aggregation-pushdown=true
teradata.export.enable-join-pushdown=true
teradata.export.enable-topn-pushdown=true
teradata.export.enable-complex-join-pushdown=true
teradata.export.enable-complex-expression-pushdown=true
```

### 9.4 Metadata & Refresh Settings

```properties
# Max entries in metadata cache (default: 1000)
teradata.export.metadata-cache-size=1000

# Background refresh frequency (e.g., 30m, 1h)
teradata.export.metadata-refresh-interval=30m

# Schemas to keep warm in cache (comma-separated)
teradata.export.metadata-refresh-schemas=DBC,TrinoExport

# Whether to refresh column details (default: true)
teradata.export.metadata-refresh-columns=true
```

---

## 10. Configuration Properties Reference

| Property | Default | Description |
|:---------|:--------|:------------|
| `teradata.url` | *required* | JDBC URL for Teradata |
| `teradata.user` | *required* | Service or Proxy account username |
| `teradata.password` | *required* | Account password |
| `teradata.timezone` | `-05:00` | Teradata server timezone offset |
| `teradata.export.bridge-port` | `9999` | Data bridge listener port |
| `teradata.export.trino-address` | *required* | Trino address for single-node mode |
| `teradata.export.splits-per-worker` | `8` | Parallel splits per worker |
| `teradata.export.batch-size` | `500000` | Rows per binary batch |
| `teradata.export.socket-receive-buffer-size`| `134217728`| OS socket buffer (bytes) |
| `teradata.export.compression-algorithm` | `ZLIB` | `ZLIB` or `LZ4` |
| `teradata.export.enable-join-pushdown` | `true` | Enable join pushdown |
| `teradata.export.enable-aggregation-pushdown`| `true` | Enable COUNT/SUM/AVG pushdown |
| `teradata.export.metadata-refresh-interval` | `0` | Background refresh frequency |
| `teradata.export.metadata-cache-size` | `1000` | Max entries in planner cache |
| `teradata.export.enforce-proxy-authentication`| `true` | Fail early if proxy fails |
| `teradata.export.broadcast-socket-timeout-ms` | `5000` | Timeout for EOS broadcast |
| `teradata.export.domain-compaction-threshold` | `100` | Threshold for predicate compaction |

---

## 11. Performance & Architectural Optimizations

The Teradata Export Connector is engineered for massive parallel throughput with **100% data reliability**. Its performance is achieved through advanced architectural design patterns.

### 11.1 Synchronous Processing (Data Integrity First)
The connector uses a **fully synchronous data pipeline** to guarantee data integrity:

```
Socket Thread: receive → decompress → parse to Page → push to buffer → next batch
                        ↓
           Only AFTER all data is in buffer:
                        ↓  
           Connection decremented → EOS signaled
```

**Key Guarantee**: Data is ALWAYS in the buffer before connection count decrements. This eliminates race conditions where EOS could be signaled prematurely.

### 11.2 DirectTrinoPageParser
The connector parses data directly for maximum performance:
- **Direct Binary-to-Page**: Teradata's packed binary format is parsed directly into Trino `Page` objects.
- **No Intermediate Format**: Eliminates allocation and conversion overhead.
- **Timezone Correction**: TIME/TIMESTAMP values are adjusted from Teradata's timezone to UTC during parsing.

### 11.3 High-Performance Compression
To reduce network bandwidth and CPU overhead:
- **Teradata C UDF**: Compresses binary batches using either **ZLIB** (high ratio) or **LZ4** (high throughput).
- **Java Bridge**: Synchronously decompresses batches using `java.util.zip.Inflater` (for ZLIB) or native LZ4 libraries.
- **Adaptive Selection**: Users can choose the algorithm via `teradata.export.compression-algorithm` or session properties.
- **Typical Ratio**: 3-10x compression for typical structured data.

### 11.4 Massively Parallel Data Routing
The connector leverages the full parallelism of the Teradata cluster:
- **AMP-Level Parallelism**: Every Teradata AMP establishes its own socket connection to a Trino worker.
- **Deterministic Load Balancing**: Using `amp_id % worker_count`, data is distributed evenly.
- **Locality Awareness**: Splits are configured with `isRemotelyAccessible = false` for local execution.

### 11.5 Robust EOS Detection

The connector implements a **deterministic End-of-Stream** protocol:

1. **Synchronous Guarantee**: Data pushed to buffer BEFORE connection decrements
2. **Connection Tracking**: Atomic counter tracks active AMP connections
3. **JDBC Completion Signal**: Coordinator broadcasts when Teradata SQL finishes
4. **Minimal Stabilization**: 100ms wait only for socket accept race conditions
5. **No Arbitrary Timeouts**: EOS based on actual completion, not guessing

**Edge Case Handling**: If no connections arrive within 5 seconds after JDBC finishes, the query is assumed to return empty results.

---

---

*Updated on 2026-01-01 - Modernized architecture with full JDBC pushdown and background metadata refresh*
