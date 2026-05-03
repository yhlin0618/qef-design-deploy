#!/usr/bin/env Rscript
# gen_product_attribute_bridges.R
#
# Generator for 12 per-PL bridge yamls (Layer 2 per MP157):
#   bridges/QEF_DESIGN/amz/product_attributes_{pl}.bridge.yaml
#
# Implements:
#   - DEV_R056 (Project Primary Language — R First) — this generator IS R
#   - MP157 Layer 2 (per-instance bridge yamls)
#   - MP159 Glue Authoring Boundary Immutability — Layer 1 (schema yamls)
#     remains untouched during bridge authoring; this script only writes Layer 2
#   - MP160 AI Authoring Completeness — exhaustive enumeration:
#     set(source_cols) === set(column_mapping.keys ∪ ignored_columns)
#   - DM_R065 Schema Dedup Discipline — uses each canonical field's first
#     alias (the source Chinese col) as `from_column`
#
# Usage:
#   Rscript gen_product_attribute_bridges.R
#
# Triggered by: spectra change qef-product-master-redesign Phase 9 task 9.6

suppressPackageStartupMessages({
  library(yaml)
  library(digest)
})

# ------------------------------------------------------------------------
# Helpers: NA literal detection + value_map emission
# (per spectra change qef-attribute-bridge-types-and-na-handling, #502 scope 2)
#
# Defined ABOVE path-resolution / IO so the test harness in
# 98_test/etl/test_detect_na_literals.R can source them in isolation
# without triggering yaml::read_yaml() on unresolved paths.
# ------------------------------------------------------------------------

# Canonical NA literals recognized by the generator. Case-sensitive — see
# design.md "NA 偵測:case-sensitive on canonical literals" decision.
.NA_CANONICAL_LITERALS <- c("NA", "N/A", "未填")

#' Detect whether a column's unique values contain any NA literal that
#' SHALL be mapped to YAML null in the bridge yaml's `value_map:`.
#'
#' Returns TRUE when:
#'   - any unique trimmed value matches `.NA_CANONICAL_LITERALS`, OR
#'   - the column mixes trimmed-empty values with at least one non-empty
#'     non-NA-literal value (per the trimmed-empty exception in design.md).
#' Returns FALSE when:
#'   - column has no NA literal AND no trimmed empty (e.g. pure 0/1, pure
#'     categorical), OR
#'   - column is fully empty (every value is "" after trim) — handled by
#'     `apply_fallback` or `ignored_columns`, not value_map.
#'
#' Pure function — does not modify input.
detect_na_literals <- function(values) {
  if (is.null(values)) return(FALSE)
  vals_chr <- as.character(values)
  trimmed <- trimws(vals_chr)
  uniq <- unique(trimmed)
  na_hits <- intersect(uniq, .NA_CANONICAL_LITERALS)
  if (length(na_hits) > 0) return(TRUE)
  # Trimmed-empty exception: only counts as NA when column has at least one
  # non-empty non-NA value (i.e., empties represent missing within an otherwise
  # populated column, not a fully empty column).
  has_empty <- any(uniq == "")
  has_real  <- any(uniq != "" & !uniq %in% .NA_CANONICAL_LITERALS)
  has_empty && has_real
}

#' Build a value_map list for a column's values, suitable for emission as a
#' bridge yaml `value_map:` mapping. Each detected NA literal (and trimmed
#' empty if it qualifies via the exception) becomes a key mapping to NULL
#' (yaml::as.yaml emits NULL as YAML `null`).
#'
#' Returns NULL when no value_map should be emitted (no NA detected, or
#' column fully empty).
build_value_map_for_col <- function(values) {
  if (!detect_na_literals(values)) return(NULL)
  vals_chr <- as.character(values)
  trimmed  <- trimws(vals_chr)
  uniq     <- unique(trimmed)
  keys <- intersect(uniq, .NA_CANONICAL_LITERALS)
  has_empty <- any(uniq == "")
  has_real  <- any(uniq != "" & !uniq %in% .NA_CANONICAL_LITERALS)
  if (has_empty && has_real) keys <- c(keys, "")
  out <- vector("list", length(keys))
  names(out) <- keys
  out  # each element is NULL (== YAML null after yaml::as.yaml)
}

# ----- end of helpers -----

# ============================================================================
# Main generator (#510): wrap top-level into function so autoinit() blanket
# sweep can source() this file without triggering yaml/csv read at import
# time. MP044 Functor-Module Correspondence — source defines functor; entry
# guard at end applies it only on Rscript invocation.
# ============================================================================
generate_bridges <- function() {

# ------------------------------------------------------------------------
# Resolve paths
# ------------------------------------------------------------------------
script_dir <- if (interactive()) {
  "/Users/che/Library/CloudStorage/Dropbox/che_workspace/projects/ai_martech/l4_enterprise/shared/global_scripts/01_db/raw_schema/_authoring/companies/QEF_DESIGN"
} else {
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- args[grepl("^--file=", args)]
  if (length(file_arg) > 0) dirname(normalizePath(sub("^--file=", "", file_arg[1])))
  else normalizePath(".")
}

# Project root contains QEF_DESIGN/data/database_to_csv/raw_data
find_project_root <- function(start) {
  d <- normalizePath(start, mustWork = FALSE)
  for (i in 1:25) {
    if (file.exists(file.path(d, "QEF_DESIGN", "data", "database_to_csv",
                              "raw_data", "df_product_profile_blb.csv"))) return(d)
    parent <- dirname(d); if (parent == d) break; d <- parent
  }
  stop("Could not find project root from ", start)
}
project_root <- find_project_root(script_dir)
csv_dir      <- file.path(project_root, "QEF_DESIGN", "data",
                          "database_to_csv", "raw_data")
schema_yaml  <- file.path(script_dir, "product_attribute_schemas.yaml")
hash_fn_path <- file.path(project_root, "shared", "global_scripts",
                          "05_etl_utils", "glue", "fn_hash_prerawdata_schema.R")
# Bridges layer is at _authoring/bridges/{COMPANY}/{platform}/ — not under
# companies/{COMPANY}/. Resolve up 3 levels from script_dir (companies/QEF_DESIGN/)
# back to _authoring/, then down into bridges/QEF_DESIGN/amz/.
authoring_root <- normalizePath(file.path(script_dir, "..", ".."),
                                 mustWork = TRUE)
out_dir      <- file.path(authoring_root, "bridges", "QEF_DESIGN", "amz")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

cat(sprintf("project_root: %s\n", project_root))
cat(sprintf("schema_yaml:  %s\n", schema_yaml))
cat(sprintf("out_dir:      %s\n", out_dir))

source(hash_fn_path)

# ------------------------------------------------------------------------
# Load schema (12 datatypes)
# ------------------------------------------------------------------------
schema <- yaml::read_yaml(schema_yaml)
pl_codes <- sub("^product_attributes_", "", names(schema))
stopifnot(length(pl_codes) == 12)

# ETL meta cols are added at ETL time, not at canonical level — list in ignored_columns
etl_meta_cols <- c("etl_import_source", "etl_import_timestamp",
                   "etl_phase", "etl_product_line_id")

# ------------------------------------------------------------------------
# Helper: read CSV columns + types for fingerprinting
# ------------------------------------------------------------------------
read_csv_schema <- function(csv_path) {
  if (!file.exists(csv_path)) stop("CSV not found: ", csv_path)
  # Read 1: default na.strings (= c("NA")) — used for type inference + fingerprinting.
  # This matches #501 D3 baseline so fingerprints stay stable.
  df <- read.csv(csv_path, stringsAsFactors = FALSE, check.names = FALSE,
                 fileEncoding = "UTF-8")
  # Strip BOM from first column name if present
  names(df)[1] <- sub("^﻿", "", names(df)[1])
  # Strip surrounding quotes from any col names that escaped
  names(df) <- gsub('^"|"$', "", names(df))
  cols  <- names(df)
  # Per spectra change qef-product-attribute-schema-fix (#501 D3): use the
  # all-NA-logical-aware helper so fingerprints stay stable across the
  # all-NA-to-typed-value transition. infer_column_types_for_fingerprint()
  # is exported from fn_hash_prerawdata_schema.R (sourced above).
  types <- if (exists("infer_column_types_for_fingerprint")) {
    unname(infer_column_types_for_fingerprint(df))
  } else {
    # Fallback for older fn_hash_prerawdata_schema.R versions
    vapply(df, function(x) class(x)[[1]], character(1))
  }
  # Read 2: na.strings = character(0) — used for cell-level value scanning.
  # This preserves source literals like "NA" / "N/A" so detect_na_literals()
  # can find them (per #502 scope 2). Cannot replace Read 1 because changing
  # na.strings flips type inference for numeric-with-NA columns and would
  # destabilize fingerprints. Two reads is a deliberate cost paid for
  # fingerprint stability + literal-faithful scanning.
  df_raw <- read.csv(csv_path, stringsAsFactors = FALSE, check.names = FALSE,
                     fileEncoding = "UTF-8", na.strings = character(0),
                     colClasses = "character")
  names(df_raw)[1] <- sub("^﻿", "", names(df_raw)[1])
  names(df_raw) <- gsub('^"|"$', "", names(df_raw))
  list(cols = cols, types = types, nrow = nrow(df), df = df, df_raw = df_raw)
}

# ------------------------------------------------------------------------
# Generate one bridge per PL
# ------------------------------------------------------------------------
generated_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
bridges_made <- character(0)

for (pl in pl_codes) {
  datatype_name <- paste0("product_attributes_", pl)
  dt <- schema[[datatype_name]]
  csv_path <- file.path(csv_dir, sprintf("df_product_profile_%s.csv", pl))

  src <- read_csv_schema(csv_path)
  fp <- hash_prerawdata_schema(src$cols, src$types)

  # Build column_mapping from each canonical field's first alias.
  # Per #502 scope 2: also scan source col values and emit value_map when
  # NA literals are detected. `src$df` is the raw data.frame from
  # read_csv_schema(); `from_col` is the source column name (Chinese alias).
  build_column_mapping <- function(fields_map) {
    result <- list()
    for (canonical_name in names(fields_map)) {
      f <- fields_map[[canonical_name]]
      aliases <- f$aliases
      if (is.null(aliases) || length(aliases) == 0) {
        # Should never happen — generator enforced aliases in 9.2
        next
      }
      from_col <- as.character(aliases[[1]])
      entry <- list(from_column = from_col)
      # NA-literal scanning (per spectra change qef-attribute-bridge-types-and-na-handling
      # #502 scope 2). Use src$df_raw (the literal-preserving read), not
      # src$df (which has read.csv's default NA-conversion applied). Only
      # scan if the source CSV actually has the column — if not, MP160
      # enumeration check downstream will surface the mismatch.
      if (!is.null(src$df_raw) && from_col %in% names(src$df_raw)) {
        vmap <- build_value_map_for_col(src$df_raw[[from_col]])
        if (!is.null(vmap)) entry$value_map <- vmap
      }
      result[[canonical_name]] <- entry
    }
    result
  }
  column_mapping <- c(
    build_column_mapping(dt$required_fields),
    build_column_mapping(dt$optional_fields)
  )

  # MP160: build ignored_columns = source_cols - mapped_cols
  mapped_source_cols <- vapply(column_mapping,
                                function(x) as.character(x$from_column),
                                character(1))
  unaccounted <- setdiff(src$cols, mapped_source_cols)
  # ETL meta cols are explicitly ignored
  expected_ignored <- intersect(unaccounted, etl_meta_cols)
  unexpected_unaccounted <- setdiff(unaccounted, etl_meta_cols)

  if (length(unexpected_unaccounted) > 0) {
    stop(sprintf("MP160 violation in %s: %d source cols neither mapped nor ETL-meta: %s",
                 pl, length(unexpected_unaccounted),
                 paste(unexpected_unaccounted, collapse = ", ")))
  }

  ignored_columns <- as.list(expected_ignored)

  # Assemble bridge yaml structure
  bridge <- list(
    prerawdata_source = list(
      company = "QEF_DESIGN",
      platform = "amz",
      source_type = "csv",
      source_uri = sprintf("QEF_DESIGN/data/database_to_csv/raw_data/df_product_profile_%s.csv", pl),
      sheet_name = NULL,  # CSV, no sheet
      schema_fingerprint = list(
        algorithm = fp$algorithm,
        value = fp$value,
        fingerprinted_against = sprintf("df_product_profile_%s.csv (full read, %d rows)",
                                        pl, src$nrow),
        fingerprinted_at = generated_at
      )
    ),
    canonical_target = list(
      datatype = datatype_name,
      platform = "amz",
      table = sprintf("df_amz_%s___raw", datatype_name),
      # Per MP161 (#499): per-PL product_attribute_* schemas live under
      # companies/QEF_DESIGN/. The validator's schema-aware check looks here
      # for canonical fields. Using the old top-level path emits dead reference.
      schema_yaml_path = "01_db/raw_schema/_authoring/companies/QEF_DESIGN/product_attribute_schemas.yaml",
      platform_extension_yaml_path = "01_db/raw_schema/_authoring/platform_extensions/amz_extensions.yaml"
    ),
    column_mapping = column_mapping,
    ignored_columns = ignored_columns,
    generated_at = generated_at,
    generated_by = "gen_product_attribute_bridges.R (qef-product-master-redesign 9.6, #460)",
    reviewed_by = "REQUIRES_HUMAN_REVIEW"  # human reviewer must replace before commit
  )

  # Build header comment block
  header <- c(
    sprintf("# Bridge: QEF_DESIGN / amz / %s", datatype_name),
    "# Spectra change: qef-product-master-redesign Phase 9 task 9.6 (#460)",
    "# Bound by MP102 v1.2 per MP156 Two-Tier Normativity (Category A)",
    "#",
    sprintf("# Generated by gen_product_attribute_bridges.R against schema %s,", basename(schema_yaml)),
    sprintf("# at %s. Source CSV: %s.", generated_at, basename(csv_path)),
    "#",
    "# MP159 (Glue Authoring Boundary Immutability) — this file is Layer 2.",
    "# MP160 (AI Authoring Completeness) — every source col is either mapped",
    "#   or in ignored_columns (ETL meta).",
    "# DEV_R056 (R-First) — generator is R, not Python.",
    "#",
    "# IMPORTANT: reviewed_by is a placeholder. A human reviewer SHALL update",
    "# this field with their identifier before this bridge is consumed.",
    ""
  )

  out_path <- file.path(out_dir, sprintf("%s.bridge.yaml", datatype_name))
  yaml_text <- yaml::as.yaml(bridge, indent = 2)
  writeLines(c(header, yaml_text), out_path)
  bridges_made <- c(bridges_made, out_path)

  cat(sprintf("  + %s (mapped=%d, ignored=%d, total=%d)\n",
              datatype_name,
              length(column_mapping), length(ignored_columns),
              length(column_mapping) + length(ignored_columns)))
}

cat(sprintf("\n✓ Generated %d bridge yamls in %s\n",
            length(bridges_made), out_dir))

  invisible(bridges_made)
}  # end generate_bridges()

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
#     If a user types `source('gen_product_attribute_bridges.R')` in the
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
  generate_bridges()
}
