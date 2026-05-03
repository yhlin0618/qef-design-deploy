# raw_schema — ETL Schema Authoring + DDL Codegen + Per-Company Bridges

This directory is the canonical home for everything related to **rawdata.duckdb** schema definition, generation, and per-company bridge configuration. It implements the architecture established by the `glue-layer-prerawdata-bridge` spectra change (#489), bound by amended **MP102** under the **MP156 Two-Tier Normativity** mechanism.

## Three sub-directories

```
01_db/raw_schema/
├── _authoring/      ← Tier-2 normative spec yaml/R authoring source-of-truth
├── _generated/      ← SQL DDL artifact, codegen output (DuckDB CREATE TABLE)
└── _build.R         ← orchestrator that reads _authoring/ → produces _generated/

(within _authoring/, after Phase 5 of #489 ships:)
01_db/raw_schema/_authoring/bridges/{COMPANY}/{platform}/{source}.bridge.yaml
                                              ← per-company bridge mapping spec
                                              ← LLM codegen + human review + git tracked
```

### `_authoring/` — Spec source-of-truth (Tier-2 normative)

Contains the canonical schema spec files bound by amended MP102:

- **`core_schemas.yaml`** — cross-platform required fields per datatype (sales / customers / orders / products / reviews) with description, aliases, pattern, fallback rules, coercion
- **`schema_registry.yaml`** — central registry tracking ETL completion status per platform × datatype
- **`transformed_schemas.yaml`** — transformed-layer schemas
- **`IMPLEMENTATION_GUIDE.md`** — how to use the schema spec for ETL implementation
- **`platform_extensions/{platform}_extensions.yaml`** — per-platform extensions (cbz, eby, amz, shp, official_website)
- **`r_definitions/SCHEMA_*.R`** — programmatic R schema definitions (poisson, position, customer profile, time series, AI translation cache)
- **`bridges/{COMPANY}/{platform}/{source}.bridge.yaml`** (added in Phase 5) — per-company prerawdata → canonical schema mapping spec

Changes to files here trigger MP102 Change Discipline: DOC_R009 三層同步 + IC_P002 cross-company verification + commit message `Verified: schema change against {N} consuming companies` trailer.

### `_generated/` — Generated SQL DDL (committed artifact)

Output of `_build.R`, organized as:

```
_generated/
├── core/{sales,customers,orders,products,reviews}.sql
└── platforms/{cbz,eby,amz,shp,official_website}/*.sql
```

These files are **committed to git** (not gitignored): they are the schema enforcement layer consumed by ETL pipelines via `CREATE TABLE FROM`. DuckDB engine rejects mismatched INSERT against these definitions (NOT NULL / CHECK / pattern constraints).

To regenerate after editing `_authoring/`:

```bash
Rscript shared/global_scripts/01_db/raw_schema/_build.R
```

The build is reproducible: running twice against unchanged `_authoring/` produces byte-identical output.

### `_build.R` — Orchestrator

Reads all yaml files under `_authoring/` and writes `_generated/*.sql` via the codegen tool `01_db/generate_create_table_query/fn_generate_create_table_query.R` (extended in Phase 3 of #489 to support `from_yaml = TRUE` mode).

### `bridges/` (created in Phase 5 of #489)

Per-company bridge mapping specs bridging prerawdata sources (GSheet / xlsx / CSV / API) to canonical raw tables. Each bridge yaml is:

- LLM-codegen produced (via `/glue-bridge` skill)
- Human-reviewed (`reviewed_by` field requires a real human identifier)
- Git-tracked (Tier-2 normative under MP102 binding)
- Consumed by `fn_glue_bridge.R` (in `05_etl_utils/glue/`) which runs deterministic mapping at production time

## Where this lives in the architecture

```
┌──────────────────────────────────────────────────────────────────┐
│ prerawdata (per-company GSheet / xlsx / API)                      │
│  ↓                                                                │
│ glue bridge (this directory's bridges/) — per-company mapping     │
│  ↓                                                                │
│ rawdata.duckdb (canonical, DDL-enforced from this directory)      │
│  ↓                                                                │
│ ETL 6-layer (raw → staged → transformed → processed → cleansed → app_data) │
│  ↓                                                                │
│ DRV layer (e.g., fn_resolve_company_product_master)              │
│  ↓                                                                │
│ Shiny apps                                                        │
└──────────────────────────────────────────────────────────────────┘
```

## Related principles

- **MP102** (ETL Output Standardization Principle) — binds the files in this directory; defines Change Discipline
- **MP156** (Two-Tier Normativity) — formalizes Tier-2 implementation normativity that this directory exemplifies
- **MP155** (Minimize Human Input) — informs the field_class system (System-Sourced / Human-Decided / System-Suggested) used in some authoring yamls
- **MP058** (Database Table Creation Strategy) — codegen pattern via `fn_generate_create_table_query.R`
- **DOC_R009** (Principle Triple-Layer Sync) — applies when MP102 amendment changes
- **IC_P002** (Cross-Company Verification) — applies when Category A / B Tier-2 files in `_authoring/` change
