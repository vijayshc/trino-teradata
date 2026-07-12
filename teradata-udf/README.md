# Teradata UDF: ExportToTrino

C table operator that serializes AMP-local rows into the binary bridge protocol
and streams them to Trino workers.

## Files

| File | Role |
|------|------|
| `export_to_trino.c` | Table operator + contract function |
| `lz4.c` / `lz4.h` | LZ4 compression (BSD 2-Clause) |
| `include/sqltypes_td.h` | **Local mock** for syntax checks only |

On Teradata, the UDF is compiled by the database against the platform headers
shipped with the Teradata software install (not this mock header).

## Registration

From the repo root (with BTEQ available and network access to Teradata):

```bash
export TD_HOST=...
export TD_LOGON_USER=...
export TD_LOGON_PASSWORD=...
export TD_UDF_DATABASE=TrinoExport
# Path must be readable by the Teradata node(s):
export UDF_SRC_DIR="$(pwd)/teradata-udf"

./scripts/register_udf.sh
RUN_BTEQ=1 ./scripts/register_udf.sh
```

## Protocol notes

- Column 0 of the exported stream carries the **routing PID** used for
  deterministic multi-worker EOS accounting.
- Compression algorithm is negotiated via UDF parameters from the connector.
- See `docs/TECHNICAL_GUIDE_TDEXPORT.md` and `docs/eos-architecture-diagram.md`.
