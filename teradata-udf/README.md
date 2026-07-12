# Teradata UDF: ExportToTrino

C table operator that serializes AMP-local rows into the connector’s binary
bridge protocol and streams them to Trino workers.

## Files

| File | Role |
|------|------|
| `export_to_trino.c` | Table operator + contract function |
| `lz4.c` / `lz4.h` | LZ4 compression (**BSD 2-Clause**) |
| `include/sqltypes_td.h` | **Local mock** for offline syntax checks only |

On Teradata, the function is compiled by the database against **platform**
headers from the Teradata installation—not this mock header.

## Routing & EOS

Each AMP computes:

```text
routing_id = getpid() XOR (getpid() >> 8)
worker_idx = routing_id % worker_count
```

The same `routing_id` is returned to the coordinator so Java can compute
**per-worker expected connection counts** (deterministic EOS). Java must use
**unsigned** 32-bit modulo. See [docs/eos.md](../docs/eos.md).

## Registration

From the **repository root** (BTEQ available, network to Teradata):

```bash
export TD_HOST=...
export TD_LOGON_USER=...
export TD_LOGON_PASSWORD=...
export TD_UDF_DATABASE=TrinoExport
# Path must be readable by Teradata node(s):
export UDF_SRC_DIR="$(pwd)/teradata-udf"

./scripts/register_udf.sh
RUN_BTEQ=1 ./scripts/register_udf.sh
```


## Related docs

- [docs/architecture.md](../docs/architecture.md)
- [docs/installation.md](../docs/installation.md)
- [docs/TECHNICAL_GUIDE_TDEXPORT.md](../docs/TECHNICAL_GUIDE_TDEXPORT.md)
