#!/usr/bin/env Rscript
# gen_product_attribute_schemas.R
#
# Generator for `product_attribute_schemas.yaml` — adds 12 per-PL attribute datatypes
# to the QEF product master schema. Reads:
#   - df_product_line.csv (canonical product line registry)
#   - QEF_DESIGN/data/database_to_csv/raw_data/df_product_profile_*.csv (12 source CSVs)
#   - product_attribute_translations.yaml (Chinese -> English canonical map)
# Emits:
#   - product_attribute_schemas.yaml (12 datatypes, full enumeration per MP160)
#
# Implements:
#   - DEV_R056 (Project Primary Language — R First) — this generator IS R, not Python
#   - MP160 (AI Authoring Completeness) — exhaustive enumeration of all 12 PLs in one pass
#   - MP157 (Two-Layer YAML Authoring) — Layer 1 canonical schema authoring
#   - MP158 (Glue-Driven Raw Layer Extensibility) — Step 1 of 3-step extensibility
#   - SO_R038 (Schema Authoring YAML Naming) — snake_case lowercase ASCII fields
#   - DM_R028 (ETL Data Type Separation) — datatype naming `product_attributes_{pl_id}`
#   - MP029 (No Fake Information) — translation map MUST cover every source col;
#     missing translation = STOP, not silent fallback
#
# Usage:
#   Rscript gen_product_attribute_schemas.R
#
# Triggered by: spectra change qef-product-master-redesign Phase 9 task 9.2 (#460)

suppressPackageStartupMessages({
  has_yaml  <- requireNamespace("yaml",  quietly = TRUE)
  has_readr <- requireNamespace("readr", quietly = TRUE)
})
if (!has_yaml || !has_readr) {
  stop("Missing required packages. Install: yaml, readr.")
}

# ============================================================================
# Main generator (#510): wrap top-level into function so autoinit() blanket
# sweep can source() this file without triggering yaml/csv read at import
# time. MP044 Functor-Module Correspondence — source defines functor; entry
# guard at end applies it only on Rscript invocation.
# ============================================================================
generate_schemas <- function() {

# ------------------------------------------------------------------------
# Resolve paths (script lives in `_authoring/`; CSVs in QEF_DESIGN data dir)
# ------------------------------------------------------------------------

script_dir <- if (interactive()) {
  "/Users/che/Library/CloudStorage/Dropbox/che_workspace/projects/ai_martech/l4_enterprise/shared/global_scripts/01_db/raw_schema/_authoring/companies/QEF_DESIGN"
} else {
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- args[grepl("^--file=", args)]
  if (length(file_arg) > 0) {
    dirname(normalizePath(sub("^--file=", "", file_arg[1])))
  } else {
    normalizePath(".")
  }
}

# Walk up from _authoring/ to find l4_enterprise project root
find_project_root <- function(start) {
  d <- normalizePath(start, mustWork = FALSE)
  for (i in 1:25) {
    if (dir.exists(file.path(d, "QEF_DESIGN")) &&
        file.exists(file.path(d, "QEF_DESIGN", "data", "database_to_csv",
                              "raw_data", "df_product_profile_blb.csv"))) {
      return(d)
    }
    parent <- dirname(d)
    if (parent == d) break
    d <- parent
  }
  stop("Could not find project root (l4_enterprise) from script_dir = ", start)
}

project_root <- find_project_root(script_dir)
csv_dir      <- file.path(project_root, "QEF_DESIGN", "data",
                          "database_to_csv", "raw_data")
pl_csv       <- file.path(project_root, "QEF_DESIGN", "data", "app_data",
                          "parameters", "scd_type1", "df_product_line.csv")
trans_yaml   <- file.path(script_dir, "product_attribute_translations.yaml")
out_yaml     <- file.path(script_dir, "product_attribute_schemas.yaml")

cat(sprintf("project_root: %s\n", project_root))
cat(sprintf("csv_dir:      %s\n", csv_dir))
cat(sprintf("translations: %s\n", trans_yaml))
cat(sprintf("output:       %s\n", out_yaml))

# ------------------------------------------------------------------------
# Load product line registry (12 PLs)
# ------------------------------------------------------------------------

pl_registry <- readr::read_csv(pl_csv, show_col_types = FALSE)
# Skip the "all" pseudo-row
pl_registry <- pl_registry[pl_registry$product_line_id != "all", ]
pl_codes <- pl_registry$product_line_id
stopifnot(length(pl_codes) == 12)
cat(sprintf("\nFound %d product lines: %s\n",
            length(pl_codes), paste(pl_codes, collapse = ", ")))

# ------------------------------------------------------------------------
# Load translation map
# ------------------------------------------------------------------------

trans_doc <- yaml::read_yaml(trans_yaml)
translations <- trans_doc$translations
cat(sprintf("\nLoaded %d translation entries\n", length(translations)))

# Verify every translation has the required keys
for (zh in names(translations)) {
  t <- translations[[zh]]
  if (is.null(t$canonical) || is.null(t$type) || is.null(t$description)) {
    stop(sprintf("Translation entry '%s' missing canonical/type/description", zh))
  }
  # SO_R038 enforcement: canonical name SHALL be snake_case lowercase ASCII
  if (!grepl("^[a-z][a-z0-9_]*$", t$canonical)) {
    stop(sprintf("Translation '%s' canonical='%s' violates SO_R038 (^[a-z][a-z0-9_]*$)",
                 zh, t$canonical))
  }
}

# Build inverse check: canonical names should be unique
canonical_names <- sapply(translations, `[[`, "canonical")
dup_canonical <- canonical_names[duplicated(canonical_names)]
if (length(dup_canonical) > 0) {
  stop("Duplicate canonical names in translations: ",
       paste(unique(dup_canonical), collapse = ", "))
}

# ------------------------------------------------------------------------
# Read each PL CSV, normalize headers (strip BOM)
# ------------------------------------------------------------------------

read_pl_columns <- function(pl) {
  path <- file.path(csv_dir, sprintf("df_product_profile_%s.csv", pl))
  if (!file.exists(path)) stop("CSV not found: ", path)
  # Read just header
  con <- file(path, "r", encoding = "UTF-8")
  on.exit(close(con))
  hdr_line <- readLines(con, n = 1, warn = FALSE)
  # Strip BOM
  hdr_line <- sub("^﻿", "", hdr_line)
  # Parse CSV header (handles quoted fields)
  cols <- read.csv(text = hdr_line, header = FALSE, stringsAsFactors = FALSE,
                   check.names = FALSE)
  cols_chr <- as.character(cols[1, ])
  # Strip surrounding quotes if any leaked
  cols_chr <- gsub('^"|"$', "", cols_chr)
  cols_chr
}

pl_columns <- lapply(setNames(pl_codes, pl_codes), read_pl_columns)
cat("\nPer-PL column counts:\n")
for (pl in pl_codes) {
  cat(sprintf("  %s: %d cols\n", pl, length(pl_columns[[pl]])))
}

# ------------------------------------------------------------------------
# MP029 enforcement — every source col MUST have a translation
# (ETL meta cols like etl_import_source are handled by canonical schema; skip)
# ------------------------------------------------------------------------

etl_meta_cols <- c("etl_import_source", "etl_import_timestamp",
                   "etl_phase", "etl_product_line_id")

all_source_cols <- unique(unlist(pl_columns, use.names = FALSE))
business_cols   <- setdiff(all_source_cols, etl_meta_cols)

untranslated <- setdiff(business_cols, names(translations))
if (length(untranslated) > 0) {
  cat(sprintf("\n!! MP029 violation: %d source cols have no translation:\n",
              length(untranslated)))
  for (c in untranslated) cat(sprintf("    %s\n", c))
  stop("Translation map incomplete. Add entries to product_attribute_translations.yaml.")
}
cat(sprintf("\n✓ MP029 check: all %d business cols have translations\n",
            length(business_cols)))

# ------------------------------------------------------------------------
# Identify columns common to all 12 PLs (these go into required_fields,
# mirroring the thin master per task 9.2 D11 decision)
# ------------------------------------------------------------------------

all_pl_col_sets <- lapply(pl_columns, setdiff, etl_meta_cols)
common_cols <- Reduce(intersect, all_pl_col_sets)
cat(sprintf("\nCommon cols across all 12 PLs (excluding ETL meta): %d\n",
            length(common_cols)))

# ------------------------------------------------------------------------
# Build the schema yaml structure (12 datatypes, full enumeration per MP160)
# ------------------------------------------------------------------------

build_field_def <- function(zh_name) {
  t <- translations[[zh_name]]
  list(
    type = t$type,
    description = t$description,
    description_zh = zh_name,
    aliases = list(zh_name)
  )
}

# Build canonical-keyed field maps
build_required_fields <- function() {
  out <- list()
  for (zh in common_cols) {
    t <- translations[[zh]]
    # Required-field rules: identity cols are required; ratings are common but
    # may be NA in source → treat as required-with-fallback
    fdef <- build_field_def(zh)
    # Add `required: true` for identity-critical fields, `required: false` otherwise.
    # NOTE (per spectra change qef-product-attribute-schema-fix, #501 D1):
    # `sku` is intentionally OMITTED from the required list. Source data has
    # rows where SKU is genuinely absent (typically competitor product rows).
    # Primary key is [asin, sales_platform], not [sku], so DDL does not enforce
    # NOT NULL. NULL on competitor rows is the honest representation; using a
    # sentinel like UNKNOWN_SKU would fabricate audit-trail evidence per MP154.
    if (t$canonical %in% c("brand", "asin", "product_name",
                            "sales_platform")) {
      fdef$required <- TRUE
    } else {
      fdef$required <- FALSE
    }
    out[[t$canonical]] <- fdef
  }
  out
}

build_optional_fields <- function(pl) {
  pl_specific <- setdiff(all_pl_col_sets[[pl]], common_cols)
  out <- list()
  for (zh in pl_specific) {
    t <- translations[[zh]]
    fdef <- build_field_def(zh)
    fdef$required <- FALSE
    out[[t$canonical]] <- fdef
  }
  out
}

# Build the 12 datatypes
datatypes <- list()
for (pl in pl_codes) {
  pl_meta <- pl_registry[pl_registry$product_line_id == pl, ]
  datatype_name <- paste0("product_attributes_", pl)

  required_fields <- build_required_fields()
  optional_fields <- build_optional_fields(pl)

  cat(sprintf("  %s: %d required + %d optional = %d total cols\n",
              datatype_name,
              length(required_fields), length(optional_fields),
              length(required_fields) + length(optional_fields)))

  datatypes[[datatype_name]] <- list(
    description = sprintf("Per-PL product attributes for %s (%s)",
                          pl_meta$product_line_name_chinese,
                          pl_meta$product_line_name_english),
    description_zh = sprintf("產品線 %s 的屬性資料表",
                             pl_meta$product_line_name_chinese),
    table_pattern = sprintf("df_{platform}_%s___raw", datatype_name),
    physical_realization = "raw_layer",
    product_line_id = pl,
    product_line_name_zh = pl_meta$product_line_name_chinese,
    product_line_name_en = pl_meta$product_line_name_english,
    source_tab = pl_meta$comment_property_sheet_tab,
    primary_key = c("asin", "sales_platform"),
    required_fields = required_fields,
    optional_fields = optional_fields
  )
}

# ------------------------------------------------------------------------
# MP160 mechanical equality check: every source col SHALL be in the schema
# ------------------------------------------------------------------------

cat("\n=== MP160 exhaustive enumeration check ===\n")
for (pl in pl_codes) {
  dt <- datatypes[[paste0("product_attributes_", pl)]]
  all_canonical <- c(names(dt$required_fields), names(dt$optional_fields))
  pl_business_cols <- setdiff(pl_columns[[pl]], etl_meta_cols)
  pl_canonical <- sapply(pl_business_cols, function(zh) translations[[zh]]$canonical)
  missing <- setdiff(pl_canonical, all_canonical)
  if (length(missing) > 0) {
    stop(sprintf("MP160 violation in %s: %d cols not enumerated: %s",
                 pl, length(missing), paste(head(missing, 5), collapse = ", ")))
  }
  cat(sprintf("  %s: ✓ all %d source cols enumerated\n",
              paste0("product_attributes_", pl), length(pl_canonical)))
}

# ------------------------------------------------------------------------
# Emit yaml file with binding header (per SO_R038 Layer 1 binding rule)
# ------------------------------------------------------------------------

binding_header <- c(
  "# product_attribute_schemas.yaml",
  "#",
  "# 12 per-PL attribute datatypes for QEF product master, generated from",
  "# QEF_DESIGN Gsheet exports (df_product_profile_*.csv).",
  "#",
  "# Bound by:",
  "#   - MP102 v1.2 (ETL Output Standardization, Tier-2 Schema Contract)",
  "#   - MP156 (Two-Tier Normativity, Category A: Schema Contract)",
  "#   - MP157 (Two-Layer YAML Authoring, Layer 1 canonical schema)",
  "#   - MP158 (Glue-Driven Raw Layer Extensibility, Step 1 of 3-step extensibility)",
  "#   - MP159 (Glue Authoring Boundary Immutability — this file is Layer 1, immutable",
  "#     during bridge authoring sessions)",
  "#   - MP160 (AI Schema/Bridge Authoring Completeness — full enumeration mandatory,",
  "#     all 12 PLs in one authoring pass)",
  "#   - SO_R038 (Schema Authoring YAML Naming — all canonical names snake_case",
  "#     lowercase ASCII)",
  "#   - DEV_R056 (Project Primary Language — R First — this file generated by",
  "#     gen_product_attribute_schemas.R, not Python)",
  "#",
  "# Generator: gen_product_attribute_schemas.R",
  "# Translation map: product_attribute_translations.yaml",
  "# Most recent significant edit: spectra change qef-product-master-redesign Phase 9 task 9.2 (#460)",
  "# Generated at: GENERATED_TIMESTAMP_PLACEHOLDER",
  "#",
  "# Change discipline: this file is GENERATED. Manual edits will be lost on next regen.",
  "# To modify a column: edit product_attribute_translations.yaml, then re-run the generator.",
  "# To add a new PL: update df_product_line.csv + add the source CSV, then re-run.",
  "",
  ""
)

# Write yaml content
yaml_body <- yaml::as.yaml(datatypes, indent = 2,
                          handlers = list(logical = function(x) {
                            result <- ifelse(x, "true", "false")
                            class(result) <- "verbatim"
                            return(result)
                          }))

# Replace timestamp placeholder
ts <- format(Sys.time(), "%Y-%m-%dT%H:%M:%S%z", tz = "UTC")
binding_header <- gsub("GENERATED_TIMESTAMP_PLACEHOLDER", ts, binding_header)

writeLines(c(binding_header, yaml_body), out_yaml)

cat(sprintf("\n✓ Generated %s (%d datatypes, %d total fields)\n",
            out_yaml,
            length(datatypes),
            sum(sapply(datatypes, function(d) {
              length(d$required_fields) + length(d$optional_fields)
            }))))

  invisible(out_yaml)
}  # end generate_schemas()

# ============================================================================
# Entry guard (#510, Decision 1 = Option C: redundant guard for defense in depth)
#
# Run generator ONLY when both predicates hold:
#   (A) sys.nframe() == 0L
#   (B) !isTRUE(.GlobalEnv$INITIALIZATION_COMPLETED)
#
# The two guards have DIFFERENT active windows (per #519 — Codex finding
# during #510 verify):
#
#   Guard (A) — active during autoinit's blanket sweep
#     `source()` pushes a calling frame, so sys.nframe() > 0L during sweep.
#     This is the guard that does the actual work blocking autoinit-time
#     execution. Without (A) alone, this file would halt autoinit on the
#     first missing input yaml.
#
#   Guard (B) — active AFTER autoinit completes
#     If a user types `source('gen_product_attribute_schemas.R')` in the
#     R console after autoinit has finished, sys.nframe() == 0L (top-level
#     console) so guard (A) lets it through, but guard (B) blocks because
#     INITIALIZATION_COMPLETED was set to TRUE at autoinit's end.
#
#   Note: during autoinit's own sweep, INITIALIZATION_COMPLETED is set
#   AFTER the load loop ends (sc_initialization_update_mode.R line 198,
#   post line 164-194 sweep). So guard (B) is effectively dead code
#   DURING the sweep itself. (A) is what keeps autoinit alive.
#
# Both together: Rscript invocation passes both (sys.nframe == 0L AND
# INITIALIZATION_COMPLETED unset) → runs. Every other source() context
# fails at least one → no execution. MP044 Functor-Module Correspondence
# is preserved.
# ============================================================================
if (sys.nframe() == 0L &&
    !isTRUE(.GlobalEnv$INITIALIZATION_COMPLETED)) {
  generate_schemas()
}
