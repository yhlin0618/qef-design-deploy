#' @file fn_glue_bridge.R
#' @use yaml
#' @use DBI
#' @use duckdb
#' @use digest
#' @requires fn_apply_mapping.R
#' @requires fn_validate_against_schema.R
#' @requires fn_hash_prerawdata_schema.R
#' @principle MP102 v1.2 (consumer of bound spec); MP156; MP154; MP029
#' @author Claude (#489 Phase 4)
#'
#' Deterministic R interpreter for prerawdata -> canonical raw mappings.
#' NO LLM CALL, NO eval/parse, NO REMOTE NETWORK ACCESS at runtime.
#' All intelligence lives in the committed bridge yaml + canonical schema yaml.

#' Run a glue bridge end-to-end
#'
#' @param company Character: company code (e.g. "QEF_DESIGN")
#' @param platform Character: platform_id (e.g. "amz")
#' @param source Character: bridge file basename without extension
#'        (e.g. "sales" -> reads sales.bridge.yaml). Often equal to datatype
#'        but bridges MAY exist per source-document (e.g. "sales_2026_q1").
#' @param prerawdata data.frame with the prerawdata source columns. The
#'        caller is responsible for loading from gsheet/xlsx/csv/api before
#'        passing in. (Loading is intentionally outside this function so
#'        the interpreter stays pure / testable.)
#' @param target_con DBI connection where the canonical raw table lives.
#'        Must already have the canonical table created (via the committed
#'        DDL in `01_db/raw_schema/_generated/`).
#' @param bridges_root Character: path to bridges/ directory. Defaults to
#'        the canonical authoring location.
#' @return List with:
#'        n_input_rows   — rows in prerawdata
#'        n_output_rows  — rows successfully INSERTed
#'        n_errors       — validation errors
#'        errors         — data.frame of per-row validation issues
#'        bridge_path    — absolute path to the bridge yaml used
#' @export
fn_glue_bridge <- function(company,
                           platform,
                           source,
                           prerawdata,
                           target_con,
                           bridges_root = NULL) {
  if (!is.character(company) || length(company) != 1) {
    stop("company must be a single character string")
  }
  if (!is.character(platform) || length(platform) != 1) {
    stop("platform must be a single character string")
  }
  if (!is.character(source) || length(source) != 1) {
    stop("source must be a single character string")
  }
  if (!is.data.frame(prerawdata)) {
    stop("prerawdata must be a data.frame")
  }
  if (!inherits(target_con, "DBIConnection")) {
    stop("target_con must be a DBI connection")
  }

  # ----- Resolve bridge yaml path -----
  if (is.null(bridges_root)) {
    # Try to locate via common path patterns
    here <- normalizePath(getwd())
    candidates <- c(
      file.path(here, "shared", "global_scripts", "01_db", "raw_schema",
                "_authoring", "bridges"),
      file.path(here, "01_db", "raw_schema", "_authoring", "bridges"),
      # Walk up from this script's location
      file.path(dirname(sys.frame(1)$ofile %||% "."),
                "..", "..", "01_db", "raw_schema", "_authoring", "bridges")
    )
    for (cand in candidates) {
      if (dir.exists(cand)) {
        bridges_root <- cand
        break
      }
    }
    if (is.null(bridges_root)) {
      stop("Could not locate bridges root; pass `bridges_root` explicitly. ",
           "Canonical: shared/global_scripts/01_db/raw_schema/_authoring/bridges/")
    }
  }

  bridge_path <- file.path(bridges_root, company, platform,
                           paste0(source, ".bridge.yaml"))
  if (!file.exists(bridge_path)) {
    stop("Bridge yaml not found: ", bridge_path,
         "\nGenerate via the glue-bridge Claude skill, then commit.")
  }

  bridge <- yaml::read_yaml(bridge_path)

  # ----- Reviewer guard (MP102 v1.3 Change Discipline) -----
  # Two acceptable forms (per spectra change `glue-bridge-self-converging-review` / #500):
  #   1. Structured (canonical): yaml mapping with required fields including
  #      findings_summary.{critical,high} == 0 AND sibling *.bridge.review.md
  #      with "## Verdict: CONVERGED" or "## Verdict: DIMINISHING" line.
  #   2. Legacy string (deprecated until 2026-07-31): non-empty string outside
  #      placeholder blacklist. After cutoff date the runtime rejects it.
  reviewed_by <- bridge$reviewed_by
  LEGACY_CUTOFF <- as.Date("2026-07-31")

  if (is.list(reviewed_by)) {
    # Structured form
    fs <- reviewed_by$findings_summary
    if (!is.list(fs)) {
      stop("Bridge yaml ", bridge_path,
           " has structured reviewed_by but missing findings_summary block. ",
           "Regenerate via /glue-bridge skill (per MP102 v1.3).")
    }
    fs_critical <- as.integer(fs$critical %||% NA_integer_)
    fs_high <- as.integer(fs$high %||% NA_integer_)
    if (is.na(fs_critical) || is.na(fs_high)) {
      stop("Bridge yaml ", bridge_path,
           " findings_summary requires integer fields `critical` and `high` ",
           "(per MP102 v1.3).")
    }
    if (fs_critical > 0L || fs_high > 0L) {
      stop("Bridge yaml ", bridge_path,
           " has unresolved findings: ", fs_critical, " CRITICAL, ",
           fs_high, " HIGH. Bridge is NOT ship-ready ",
           "(per MP102 v1.3 Change Discipline). Re-run /glue-bridge to converge.")
    }

    # Verify sibling review log exists with proper verdict
    bridge_dir <- dirname(bridge_path)
    artifact_ref <- as.character(reviewed_by$review_artifacts %||% "")
    if (!nzchar(artifact_ref)) {
      stop("Bridge yaml ", bridge_path,
           " structured reviewed_by missing review_artifacts field.")
    }
    artifact_resolved <- if (startsWith(artifact_ref, "/")) artifact_ref
                        else file.path(bridge_dir, artifact_ref)
    if (!file.exists(artifact_resolved)) {
      stop("Bridge review log not found at ", artifact_resolved,
           " (referenced by ", bridge_path, "). ",
           "Audit trail is required for runtime use (per MP102 v1.3).")
    }
    log_lines <- readLines(artifact_resolved, warn = FALSE)
    has_verdict <- any(grepl("^## Verdict:\\s*(CONVERGED|DIMINISHING)",
                             log_lines, ignore.case = TRUE))
    if (!has_verdict) {
      stop("Bridge review log ", artifact_resolved,
           " lacks `## Verdict: CONVERGED` or `## Verdict: DIMINISHING`. ",
           "Bridge is not ship-ready (per MP102 v1.3).")
    }

  } else {
    # Legacy string form (or NULL/missing)
    rb_str <- as.character(reviewed_by %||% "")
    if (!nzchar(rb_str) ||
        rb_str %in% c("AI", "ai", "REQUIRES_HUMAN_REVIEW",
                       "TBD", "unknown")) {
      stop("Bridge yaml ", bridge_path,
           " has reviewed_by = '", rb_str, "'. ",
           "A reviewer identifier is required before runtime use. ",
           "Use /glue-bridge skill to produce a structured form (MP102 v1.3) ",
           "or provide a real human identifier.")
    }
    if (Sys.Date() > LEGACY_CUTOFF) {
      stop("Bridge yaml ", bridge_path,
           " uses legacy string reviewed_by = '", rb_str,
           "' which is rejected after 2026-07-31. ",
           "Migrate via /glue-bridge skill (per MP102 v1.3).")
    }
    # else: legacy string accepted with implicit deprecation (warning emitted by validator,
    # not by runtime — runtime stays quiet to avoid spam)
  }

  # ----- Drift check (design D4) -----
  expected_fp <- bridge$prerawdata_source$schema_fingerprint
  if (is.null(expected_fp) || is.null(expected_fp$value)) {
    stop("Bridge yaml ", bridge_path,
         " missing prerawdata_source.schema_fingerprint. Regenerate.")
  }
  src_cols <- colnames(prerawdata)
  src_types <- vapply(prerawdata, function(x) class(x)[1], character(1))
  actual_fp <- hash_prerawdata_schema(src_cols, src_types)
  drift_msg <- fingerprint_diff_message(expected_fp, actual_fp)
  if (nzchar(drift_msg)) {
    stop(drift_msg)
  }

  # ----- Load canonical schema + extension -----
  schema_path <- bridge$canonical_target$schema_yaml_path %||% ""
  if (!nzchar(schema_path) || !file.exists(schema_path)) {
    # Try resolving relative to bridges_root
    schema_path <- file.path(dirname(dirname(bridges_root)),
                             "core_schemas.yaml")
  }
  if (!file.exists(schema_path)) {
    stop("Cannot resolve canonical_target.schema_yaml_path from bridge yaml")
  }
  schema_doc <- yaml::read_yaml(schema_path)
  datatype <- bridge$canonical_target$datatype
  if (is.null(datatype) || !datatype %in% names(schema_doc)) {
    stop("Bridge canonical_target.datatype '", datatype %||% "<NULL>",
         "' not found in canonical schema")
  }
  required_fields <- schema_doc[[datatype]]$required_fields

  ext_fields <- list()
  ext_path <- bridge$canonical_target$platform_extension_yaml_path %||% ""
  if (nzchar(ext_path) && file.exists(ext_path)) {
    ext_doc <- yaml::read_yaml(ext_path)
    if (datatype %in% names(ext_doc) && !is.null(ext_doc[[datatype]]$fields)) {
      ext_fields <- ext_doc[[datatype]]$fields
    }
  }

  # ----- Apply mapping -----
  mapped <- apply_mapping(
    prerawdata = prerawdata,
    column_mapping = bridge$column_mapping %||% list(),
    required_fields = required_fields,
    ext_fields = ext_fields,
    canonical_target_platform = bridge$canonical_target$platform %||%
      platform,
    pre_filter = bridge$pre_filter
  )

  # ----- Validate -----
  validation <- validate_against_schema(
    df = mapped,
    required_fields = required_fields,
    ext_fields = ext_fields
  )

  # ----- INSERT into canonical table (DDL re-validates per generated SQL) -----
  target_table <- bridge$canonical_target$table %||%
    paste0("df_", platform, "_", datatype, "___raw")

  n_output <- 0L
  if (validation$n_errors == 0) {
    DBI::dbWriteTable(target_con, target_table, mapped, append = TRUE)
    n_output <- nrow(mapped)
  }

  list(
    n_input_rows = nrow(prerawdata),
    n_output_rows = n_output,
    n_errors = validation$n_errors,
    errors = validation$errors,
    bridge_path = bridge_path,
    target_table = target_table
  )
}

`%||%` <- function(a, b) if (is.null(a)) b else a
