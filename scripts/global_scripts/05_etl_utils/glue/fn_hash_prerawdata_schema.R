#' @file fn_hash_prerawdata_schema.R
#' @use digest
#' @principle MP102 v1.2 (bound spec); MP156 (Tier-2 binding); MP029 (verify before use)
#' @author Claude (#489 Phase 4)

#' Compute schema fingerprint of a prerawdata source
#'
#' Deterministic algorithm: sort columns by name, concatenate
#' "<name>:<type>" lines with newlines, sha256 the result.
#'
#' This is the **single source of truth** for "did the prerawdata schema
#' drift since the bridge was authored?" The same algorithm is applied
#' at codegen time (skill Step 1) and at runtime (`fn_glue_bridge`
#' Step 3). A mismatch halts execution.
#'
#' @param column_names Character vector of prerawdata column names.
#' @param column_types Character vector of inferred R/SQL types
#'        (same length as column_names).
#' @return List with `algorithm = "sha256"` and `value = <hex string>`.
#' @export
hash_prerawdata_schema <- function(column_names, column_types) {
  if (!is.character(column_names) || !is.character(column_types)) {
    stop("column_names and column_types must both be character vectors")
  }
  if (length(column_names) != length(column_types)) {
    stop("column_names and column_types must have the same length: ",
         length(column_names), " vs ", length(column_types))
  }
  if (length(column_names) == 0) {
    stop("Empty schema — at least one column is required")
  }
  if (any(is.na(column_names)) || any(is.na(column_types))) {
    stop("Schema contains NA — fix prerawdata source before fingerprinting")
  }

  pairs <- paste(column_names, column_types, sep = ":")
  sorted_pairs <- sort(pairs)
  payload <- paste(sorted_pairs, collapse = "\n")

  if (!requireNamespace("digest", quietly = TRUE)) {
    stop("Package `digest` is required for sha256. install.packages('digest').")
  }
  list(
    algorithm = "sha256",
    value = digest::digest(payload, algo = "sha256", serialize = FALSE)
  )
}

#' Compare two fingerprints; produce an actionable diff message on mismatch
#'
#' @param expected_fingerprint List with `algorithm` + `value` (from bridge yaml)
#' @param actual_fingerprint List with `algorithm` + `value` (from runtime)
#' @return Character message; "" if identical
#' @export
#' Infer column types for fingerprinting, normalizing all-NA logical columns
#'
#' R's `class(x)[1]` returns `"logical"` for columns with no non-NA values
#' (the default empty-vector type). Once one real value lands, the type
#' typically flips to `"character"` (URL/text) or `"numeric"` (rank/integer),
#' which would change the fingerprint and trigger false-positive drift.
#'
#' This wrapper normalizes all-NA logical columns to `"character"` (the
#' canonical type for unknown content in R), stabilizing the fingerprint
#' across the all-NA-to-typed-value transition. Drift detection for
#' legitimate type changes (column rename, real type change between
#' non-all-NA states) is preserved.
#'
#' Bound by spectra change `qef-product-attribute-schema-fix` (#501 D3).
#'
#' @param df data.frame
#' @return named character vector, one element per column, with type tags
#'         suitable for input to hash_prerawdata_schema()
#' @export
infer_column_types_for_fingerprint <- function(df) {
  if (!is.data.frame(df)) {
    stop("infer_column_types_for_fingerprint expects a data.frame")
  }
  vapply(df, function(x) {
    cls <- class(x)[1]
    # Normalize all-NA logical columns to character (per #501 D3) —
    # avoids false-positive drift when a real value later flips the type.
    if (identical(cls, "logical") && length(x) > 0L && all(is.na(x))) {
      return("character")
    }
    cls
  }, character(1))
}

fingerprint_diff_message <- function(expected_fingerprint, actual_fingerprint) {
  if (identical(expected_fingerprint$value, actual_fingerprint$value)) {
    return("")
  }
  paste0(
    "Schema drift detected.\n",
    "  Expected fingerprint (from bridge yaml): ",
    expected_fingerprint$value, "\n",
    "  Actual fingerprint (from current source): ",
    actual_fingerprint$value, "\n",
    "Action: re-run the glue-bridge skill to regenerate the bridge yaml,\n",
    "        have a human reviewer sign off, then commit the updated yaml.\n",
    "        Do NOT bypass this check — schema drift breaks downstream DRV."
  )
}
