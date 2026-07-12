# Documentation index

| Document | Audience | Description |
|----------|----------|-------------|
| [architecture.md](architecture.md) | Everyone | Control/data plane, routing, security model |
| [eos.md](eos.md) | Implementers / operators | Deterministic end-of-stream design |
| [eos-architecture-diagram.md](eos-architecture-diagram.md) | Implementers | EOS diagrams only |
| [installation.md](installation.md) | Operators | Build, deploy, UDF, network, smoke test |
| [configuration.md](configuration.md) | Operators | Catalog property reference |
| [TECHNICAL_GUIDE_TDEXPORT.md](TECHNICAL_GUIDE_TDEXPORT.md) | Operators / implementers | Full technical guide |
| [development.md](development.md) | Contributors | SPI packaging, standards, tests |

Project root also has:

- [../README.md](../README.md) — overview & quick start  
- [../SECURITY.md](../SECURITY.md) — hardening & disclosure  
- [../CONTRIBUTING.md](../CONTRIBUTING.md) — contribution process  
- [../teradata-udf/README.md](../teradata-udf/README.md) — table operator notes  

## Doc accuracy rules

When changing code, update docs in the same change set:

1. No lab IPs, passwords, or personal home paths in tracked docs.
2. Paths must match `plugin/trino-teradata`, `teradata-udf`, `testing/trino-teradata-tests`.
3. EOS primary path is **deterministic expected counts**, not JDBC_FINISHED / short timeouts.
4. Packaging is **`trino-plugin`** + `./mvnw`, not `dependency:copy-dependencies` alone.
5. `connector.name` is `teradata_export` (underscore).
