#!/usr/bin/env Rscript
# _build.R — Orchestrator for raw_schema codegen
# Following MP102 (v1.2) bound by MP156 Two-Tier Normativity (issue #489 Phase 3)
#
# Reads `_authoring/core_schemas.yaml` + `_authoring/platform_extensions/*.yaml`
# and writes `_generated/core/*.sql` + `_generated/platforms/{platform}/*.sql`.
#
# Usage:
#   Rscript shared/global_scripts/01_db/raw_schema/_build.R
#
# Reproducibility guarantee (MP102 Change Discipline #3): running this script
# twice against an unchanged `_authoring/` produces byte-identical output in
# `_generated/`. `git diff _generated/` after a re-run on clean tree must be
# empty.
#
# What gets generated:
#   _generated/core/{sales,customers,orders,products,reviews}.sql
#       Core-only DDL (no platform extensions). Useful as a reference and
#       for downstream tools that operate on the universal layer.
#   _generated/platforms/{platform}/{datatype}.sql
#       Platform-extended DDL, with the active datatype for each platform
#       (per the platform's extension yaml).
#
# Ship gates (Phase 3):
#   - Output files are valid DuckDB DDL (parseable + executable).
#   - Output is reproducible across re-runs.
#   - At least one CHECK constraint includes the expected pattern (e.g.,
#     amz_asin column has CHECK ... ~ '^B0[A-Z0-9]{8}$' ...).

suppressPackageStartupMessages({
  if (!requireNamespace("yaml", quietly = TRUE)) {
    stop("Package `yaml` is required. install.packages('yaml').")
  }
  if (!requireNamespace("DBI", quietly = TRUE)) {
    stop("Package `DBI` is required.")
  }
  if (!requireNamespace("duckdb", quietly = TRUE)) {
    stop("Package `duckdb` is required.")
  }
})

# Levenshtein distance — base R only (no extra package dep).
# Cribbed from utils::adist; kept inline to avoid any surprise dispatch.
lev_distance <- function(a, b) {
  as.integer(utils::adist(a, b)[1L, 1L])
}

# Suggest the nearest legal key from `legal_keys` for an unknown `bad_key`.
# Returns either "" (no good suggestion) or " (did you mean 'X'?)".
suggest_key <- function(bad_key, legal_keys, max_dist) {
  if (length(legal_keys) == 0L) return("")
  dists <- vapply(legal_keys, function(k) lev_distance(bad_key, k), integer(1L))
  best <- which.min(dists)
  if (length(best) == 0L || dists[best] > max_dist) return("")
  sprintf(" (did you mean '%s'?)", legal_keys[best])
}

# Emit a single validation error.
fail_unknown_key <- function(file_label, key_path, bad_key, legal_keys,
                             max_dist) {
  hint <- suggest_key(bad_key, legal_keys, max_dist)
  stop(sprintf(
    "[meta-schema] %s: unknown key '%s' at %s%s\n  legal keys: %s",
    file_label, bad_key, key_path, hint,
    paste(legal_keys, collapse = ", ")
  ), call. = FALSE)
}

# Null-coalescing helper.
`%||%` <- function(a, b) if (is.null(a)) b else a

# ----------------------------------------------------------------------------
# Main build orchestrator (MP044 functor-module wrap, #520 — sister to
# #510 / #518: prevents top-level yaml read + stop() from halting autoinit
# sweep when this file is sourced via blanket directory sweep).
#
# All path resolution, I/O, validation, and codegen lives here. Top level
# of the file only defines pure helpers + this function + entry guard.
# ----------------------------------------------------------------------------
build_raw_schema <- function() {

# ----------------------------------------------------------------------------
# Resolve script paths (location-independent — works whether invoked from
# project root, shared/, or shared/global_scripts/)
# ----------------------------------------------------------------------------
args <- commandArgs(trailingOnly = FALSE)
file_arg <- args[grep("^--file=", args)]
if (length(file_arg) > 0) {
  script_path <- normalizePath(sub("^--file=", "", file_arg[1]))
} else {
  # Fall back to here() if interactively sourced
  script_path <- normalizePath("_build.R")
}
script_dir <- dirname(script_path)

authoring_dir  <- file.path(script_dir, "_authoring")
generated_dir  <- file.path(script_dir, "_generated")

if (!dir.exists(authoring_dir)) {
  stop("Authoring directory not found: ", authoring_dir)
}

core_yaml <- file.path(authoring_dir, "core_schemas.yaml")
if (!file.exists(core_yaml)) {
  stop("core_schemas.yaml not found at canonical path: ", core_yaml)
}

# ----------------------------------------------------------------------------
# Meta-schema validation (SO_R038 rule 6, qef-product-master-redesign 9.0d)
#
# Validates each authoring yaml against `_meta_schema.yaml`'s registered key
# set BEFORE codegen runs. Unknown keys halt the build with a Levenshtein hint
# pointing at the nearest legal key. Catches typos like `phyiscal_realization`
# that would otherwise be silently ignored by `yaml::read_yaml()`.
# ----------------------------------------------------------------------------
meta_schema_yaml <- file.path(authoring_dir, "_meta_schema.yaml")
if (!file.exists(meta_schema_yaml)) {
  stop("_meta_schema.yaml not found at canonical path: ", meta_schema_yaml,
       "\nThis file is required by SO_R038 rule 6 (registered datatype/field keys).")
}
meta <- yaml::read_yaml(meta_schema_yaml)

# Validate one yaml file against the named profile in _meta_schema.yaml.
# `profile` is "canonical_schema" or "platform_extension". Closes over `meta`.
validate_against_meta_schema <- function(yaml_path, profile) {
  prof <- meta[[profile]]
  if (is.null(prof)) {
    stop(sprintf("[meta-schema] profile '%s' not defined in _meta_schema.yaml",
                 profile), call. = FALSE)
  }
  doc <- yaml::read_yaml(yaml_path)
  file_label <- basename(yaml_path)
  max_dist <- as.integer(meta$validator$levenshtein_max_distance %||% 2L)

  # Top-level keys
  for (k in names(doc)) {
    if (!(k %in% prof$top_level_keys)) {
      fail_unknown_key(file_label, "<top-level>", k, prof$top_level_keys,
                       max_dist)
    }
  }

  # Datatype names that get full datatype-level + field-level walking.
  # Top-level keys not in this set (e.g., validation_rules / type_mapping /
  # bridge_mapping_notes in canonical schemas) have their own internal
  # structures and are validated only at the top-level membership check above.
  datatype_section_names <- c("sales", "customers", "orders", "products",
                              "reviews",
                              "company_product_extension")  # D11 SKU-keyed financial extension (#460 Phase 9)

  # Per-datatype keys + per-field keys
  for (dt_name in names(doc)) {
    if (!(dt_name %in% datatype_section_names)) next
    dt <- doc[[dt_name]]
    if (!is.list(dt) || is.null(names(dt))) next  # non-mapping section

    for (k in names(dt)) {
      if (!(k %in% prof$datatype_keys)) {
        fail_unknown_key(
          file_label, sprintf("%s.<datatype-level>", dt_name), k,
          prof$datatype_keys, max_dist
        )
      }
    }

    # Walk required_fields / optional_fields / fields (the field-bearing keys)
    field_sections <- intersect(c("required_fields", "optional_fields",
                                  "fields"), names(dt))
    for (sec in field_sections) {
      fields <- dt[[sec]]
      if (!is.list(fields)) next
      for (fname in names(fields)) {
        fdef <- fields[[fname]]
        if (!is.list(fdef)) next
        for (k in names(fdef)) {
          if (!(k %in% prof$field_keys)) {
            fail_unknown_key(
              file_label,
              sprintf("%s.%s.%s", dt_name, sec, fname), k,
              prof$field_keys, max_dist
            )
          }
        }
        # Closed-vocabulary check: type
        # Allow parameterized SQL types (VARCHAR(3), NUMERIC(10,2), etc.) by
        # checking only the base type before any open-paren.
        if (!is.null(fdef$type) &&
            length(prof$allowed_types) > 0L) {
          base_type <- sub("\\(.*$", "", as.character(fdef$type))
          if (!(base_type %in% prof$allowed_types)) {
            fail_unknown_key(
              file_label,
              sprintf("%s.%s.%s.type", dt_name, sec, fname),
              fdef$type, prof$allowed_types, max_dist
            )
          }
        }
        # Coercion sub-keys (canonical_schema only — extensions don't use coercion)
        if (!is.null(prof$coercion_keys) && is.list(fdef$coercion)) {
          for (k in names(fdef$coercion)) {
            if (!(k %in% prof$coercion_keys)) {
              fail_unknown_key(
                file_label,
                sprintf("%s.%s.%s.coercion", dt_name, sec, fname), k,
                prof$coercion_keys, max_dist
              )
            }
          }
        }
        # Fallback sub-keys
        if (!is.null(prof$fallback_keys) && is.list(fdef$fallback)) {
          for (k in names(fdef$fallback)) {
            if (!(k %in% prof$fallback_keys)) {
              fail_unknown_key(
                file_label,
                sprintf("%s.%s.%s.fallback", dt_name, sec, fname), k,
                prof$fallback_keys, max_dist
              )
            }
          }
          # Closed-vocabulary check: fallback.rule
          if (!is.null(fdef$fallback$rule) &&
              length(prof$allowed_fallback_rules) > 0L &&
              !(fdef$fallback$rule %in% prof$allowed_fallback_rules)) {
            fail_unknown_key(
              file_label,
              sprintf("%s.%s.%s.fallback.rule", dt_name, sec, fname),
              fdef$fallback$rule, prof$allowed_fallback_rules, max_dist
            )
          }
        }
      }
    }
  }
  invisible(TRUE)
}

# Source the codegen functions (sibling chapter 01_db/generate_create_table_query/)
gen_dir <- normalizePath(file.path(script_dir, "..", "generate_create_table_query"))
sql_utils_dir <- normalizePath(file.path(gen_dir, "..", "..", "14_sql_utils"))
source(file.path(sql_utils_dir, "fn_sanitize_identifier.R"))
source(file.path(sql_utils_dir, "fn_quote_identifier.R"),
       chdir = FALSE)
source(file.path(gen_dir, "fn_generate_create_table_query.R"))
source(file.path(gen_dir, "fn_generate_create_table_from_yaml.R"))

# ----------------------------------------------------------------------------
# Build plan
# ----------------------------------------------------------------------------
core_datatypes <- c("sales", "customers", "orders", "products", "reviews",
                    "company_product_extension")  # D11 SKU-keyed financial extension (#460 Phase 9 task 9.3)

# Discover platforms from platform_extensions/*_extensions.yaml
ext_dir <- file.path(authoring_dir, "platform_extensions")
ext_files <- list.files(ext_dir, pattern = "_extensions\\.yaml$",
                        full.names = TRUE)
platforms <- gsub("_extensions\\.yaml$", "", basename(ext_files))

cat(sprintf("[_build.R] Authoring dir: %s\n", authoring_dir))
cat(sprintf("[_build.R] Generated dir: %s\n", generated_dir))
cat(sprintf("[_build.R] Core datatypes (%d): %s\n",
            length(core_datatypes), paste(core_datatypes, collapse = ", ")))
cat(sprintf("[_build.R] Platforms (%d): %s\n",
            length(platforms), paste(platforms, collapse = ", ")))

# ----------------------------------------------------------------------------
# Meta-schema validation pass — runs BEFORE any codegen so unknown keys halt
# the build cleanly without leaving partial _generated/ output.
# ----------------------------------------------------------------------------
cat("\n[_build.R] === Meta-schema validation ===\n")
cat(sprintf("  -> %s (canonical_schema)\n", basename(core_yaml)))
validate_against_meta_schema(core_yaml, "canonical_schema")
for (ext_yaml in ext_files) {
  cat(sprintf("  -> %s (platform_extension)\n", basename(ext_yaml)))
  validate_against_meta_schema(ext_yaml, "platform_extension")
}
cat("  All authoring yamls conform to _meta_schema.yaml registered key set.\n")

# Ensure clean output structure (idempotent: rebuild from scratch each run
# so reproducibility test never sees stale residue).
core_out_dir <- file.path(generated_dir, "core")
plat_out_dir <- file.path(generated_dir, "platforms")
unlink(core_out_dir, recursive = TRUE)
unlink(plat_out_dir, recursive = TRUE)
dir.create(core_out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(plat_out_dir, recursive = TRUE, showWarnings = FALSE)

# ----------------------------------------------------------------------------
# 1) Core-only DDL
# ----------------------------------------------------------------------------
cat("\n[_build.R] === Core DDL ===\n")
for (dt in core_datatypes) {
  out_path <- file.path(core_out_dir, paste0(dt, ".sql"))
  sql <- generate_create_table_from_yaml(
    schema_yaml_path = core_yaml,
    datatype = dt,
    platform = NULL
  )
  writeLines(sql, out_path)
  cat(sprintf("  + %s\n", out_path))
}

# ----------------------------------------------------------------------------
# 2) Per-platform DDL
# ----------------------------------------------------------------------------
cat("\n[_build.R] === Platform DDL ===\n")
for (platform in platforms) {
  ext_yaml <- file.path(ext_dir, paste0(platform, "_extensions.yaml"))
  if (!file.exists(ext_yaml)) {
    cat(sprintf("  ! skip %s (no extension yaml)\n", platform))
    next
  }
  ext_doc <- yaml::read_yaml(ext_yaml)
  active_datatypes <- intersect(core_datatypes, names(ext_doc))
  if (length(active_datatypes) == 0) {
    cat(sprintf("  ! skip %s (no datatype sections in extension)\n", platform))
    next
  }
  plat_out <- file.path(plat_out_dir, platform)
  dir.create(plat_out, recursive = TRUE, showWarnings = FALSE)
  for (dt in active_datatypes) {
    out_path <- file.path(plat_out, paste0(dt, ".sql"))
    sql <- generate_create_table_from_yaml(
      schema_yaml_path = core_yaml,
      datatype = dt,
      platform = platform,
      platform_extension_yaml_path = ext_yaml
    )
    writeLines(sql, out_path)
    cat(sprintf("  + %s\n", out_path))
  }
}

# ----------------------------------------------------------------------------
# 3) Per-company DDL (MP161 Company-Scoped Schema Recognition, #499)
# ----------------------------------------------------------------------------
# Discover company-scoped schemas at _authoring/companies/{COMPANY}/*.yaml
# and emit their DDL to _generated/companies/{COMPANY}/platforms/{platform}/
# per MP161's directory layout rules.
#
# Replaces the previous "Per-PL attribute DDL" block that hardcoded a single
# yaml path (_authoring/product_attribute_schemas.yaml) and a single platform
# ("amz"). New design discovers schemas by directory traversal — supports
# arbitrary companies and arbitrary schema yamls per company.
#
# Filtering rule: a yaml file in _authoring/companies/{COMPANY}/ is a SCHEMA
# yaml (not a translation map) iff its top-level mapping contains at least
# one entry with `required_fields:`. Translation maps (zh-en lookup tables)
# don't have that key and are skipped.
#
# Platform: hardcoded to "amz" for v1 because the only current company-scoped
# schema (QEF_DESIGN's product_attribute_schemas) is amz-only. Future companies
# adopting cbz/eby/shp per-domain schemas would extend this iteration.
companies_dir <- file.path(authoring_dir, "companies")
if (dir.exists(companies_dir)) {
  cat("\n[_build.R] === Per-company DDL ===\n")
  company_dirs <- list.dirs(companies_dir, recursive = FALSE,
                             full.names = TRUE)
  if (length(company_dirs) == 0L) {
    cat("[_build.R] (skip) no company-scoped schemas found in companies/\n")
  }
  per_company_platform <- "amz"  # v1 default; extend when other platforms adopt
  per_company_ext_yaml <- file.path(ext_dir,
                                     paste0(per_company_platform,
                                            "_extensions.yaml"))

  for (company_dir in company_dirs) {
    company_name <- basename(company_dir)
    yaml_files <- list.files(company_dir, pattern = "\\.yaml$",
                              full.names = TRUE)

    for (yf in yaml_files) {
      # Read yaml; skip files that have no `required_fields:` at any
      # top-level entry (those are translation maps, not schemas)
      doc <- tryCatch(yaml::read_yaml(yf), error = function(e) NULL)
      if (is.null(doc) || !is.list(doc) || length(doc) == 0L) next

      schema_datatypes <- character(0)
      for (k in names(doc)) {
        if (is.list(doc[[k]]) && !is.null(doc[[k]]$required_fields)) {
          schema_datatypes <- c(schema_datatypes, k)
        }
      }
      if (length(schema_datatypes) == 0L) next  # not a schema file

      out_dir <- file.path(generated_dir, "companies", company_name,
                            "platforms", per_company_platform)
      dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

      for (dt in schema_datatypes) {
        out_path <- file.path(out_dir, paste0(dt, ".sql"))
        sql <- generate_create_table_from_yaml(
          schema_yaml_path = yf,
          datatype = dt,
          platform = per_company_platform,
          platform_extension_yaml_path = if (file.exists(per_company_ext_yaml)) per_company_ext_yaml else NULL
        )
        writeLines(sql, out_path)
        cat(sprintf("  + %s\n", out_path))
      }
    }
  }
} else {
  cat("\n[_build.R] (skip) _authoring/companies/ does not exist\n")
}

cat("\n[_build.R] Done.\n")

}  # end build_raw_schema()

# ----------------------------------------------------------------------------
# Entry guard (#520, redundant pattern matching #510 / #518)
#
# Run build_raw_schema() ONLY when invoked via direct Rscript at the
# command line, never when sourced from R (e.g., autoinit blanket sweep
# of shared/global_scripts/, or interactive `source('_build.R')`).
#
# Two redundant guards (any one alone would suffice; both for defense in depth):
#   (A) sys.nframe() == 0L       — no calling frames (true Rscript invocation)
#   (B) !INITIALIZATION_COMPLETED — autoinit hasn't fully finished yet
#
# Note (per #519 finding): during autoinit blanket sweep,
# `INITIALIZATION_COMPLETED` is set AFTER the load loop ends, so guard B
# is effectively dead during the sweep itself. Guard A (sys.nframe > 0L
# from source()) is what actually blocks autoinit-time execution. Guard B
# remains useful for the post-autoinit case where a user types
# `source("_build.R")` in the R console after autoinit completed.
# ----------------------------------------------------------------------------
if (sys.nframe() == 0L &&
    !isTRUE(.GlobalEnv$INITIALIZATION_COMPLETED)) {
  build_raw_schema()
}
