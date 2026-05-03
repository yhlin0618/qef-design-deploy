#' Validate Amazon Sales Row Integrity (#475, #506)
#'
#' Detects structurally-corrupt rows in Amazon Seller Central xlsx exports
#' (e.g., column-shifted rows where `sku` got an ASIN value, `asin` got an
#' order-status value, `quantity` got a currency code, etc.) and separates
#' them from clean rows.
#'
#' Per SO_R038 v1.1 rule 5 (Multi-Field Row Corruption, MP163-aligned):
#' a row with multiple fields simultaneously corrupted is escalated from
#' field-level sentinel to row-level coverage audit. The function does NOT
#' mutate any field — it drops the corrupt row from the clean data and
#' emits an audit record so the row is visible to the attention loop.
#'
#' Detection signal: `asin` column has a non-empty value that does not
#' match `^B[0-9A-Z]{9}$` (canonical Amazon ASIN format). Empty / NA
#' `asin` is NOT flagged here — the existing `process_amazon_sales()`
#' downstream filter already handles missing-asin rows.
#'
#' @param data data.frame after column-name normalization. Expected to have
#'   at least an `asin` column. If `asin` column is absent, returns input
#'   unchanged with empty audit (validator is a no-op for non-asin tables).
#' @param source_file Character. Source file name for audit metadata.
#'   Defaults to NA_character_.
#' @param verbose Logical. Whether to message bad-row counts. Default TRUE.
#'
#' @return list with two elements:
#'   - `clean_data`: data.frame with corrupt rows removed
#'   - `audit_rows`: data.frame of corrupt rows with metadata
#'     (etl_source_file, source_row_index, reason, detected_via,
#'     observed_asin, observed_sku, detected_at). Empty data.frame if no
#'     corrupt rows.
#'
#' @details
#' Why drop the row instead of `df$asin <- "UNKNOWN_ASIN"`:
#' Per SO_R038 rule 5, when a single source row has 2+ fields corrupt
#' simultaneously (column shift), per-field sentinels mask row-level
#' corruption. The "untouched" fields (`order_id`, `purchase_date`, etc.)
#' are still in the wrong column. Coverage audit is the correct
#' MP163-aligned escalation.
#'
#' Why not use ASIN-format-fail as the only signal: in practice, observed
#' D_RACING xlsx column shifts always have asin format failure as the
#' detectable surface signal (asin column gets order_status value), and
#' the downstream sku column always has a corresponding ASIN-format value
#' (since the source's sku slot got the ASIN). This 1-signal detector is
#' sufficient for the observed failure mode (#475).
#'
#' @examples
#' \dontrun{
#' result <- validate_amz_sales_row_integrity(
#'   data = read_excel("path/to/202411.xlsx", col_types = "text"),
#'   source_file = "202411.xlsx"
#' )
#' nrow(result$clean_data)
#' nrow(result$audit_rows)
#' }
#'
#' @export
validate_amz_sales_row_integrity <- function(data,
                                              source_file = NA_character_,
                                              verbose = TRUE) {
  empty_audit <- data.frame(
    etl_source_file = character(0),
    source_row_index = integer(0),
    reason = character(0),
    detected_via = character(0),
    observed_asin = character(0),
    observed_sku = character(0),
    detected_at = as.POSIXct(character(0)),
    stringsAsFactors = FALSE
  )

  # No-op when there's nothing to validate
  if (is.null(data) || nrow(data) == 0) {
    return(list(clean_data = data, audit_rows = empty_audit))
  }

  if (!"asin" %in% names(data)) {
    # Non-asin tables (e.g., metadata-only files) — validator does not apply
    return(list(clean_data = data, audit_rows = empty_audit))
  }

  asin_clean <- trimws(as.character(data$asin))
  asin_present <- !is.na(asin_clean) & nzchar(asin_clean)
  asin_format_ok <- grepl("^B[0-9A-Z]{9}$", asin_clean)
  bad_row <- asin_present & !asin_format_ok

  if (!any(bad_row)) {
    return(list(clean_data = data, audit_rows = empty_audit))
  }

  bad_indices <- which(bad_row)
  observed_sku <- if ("sku" %in% names(data)) {
    as.character(data$sku[bad_indices])
  } else {
    rep(NA_character_, length(bad_indices))
  }

  audit_rows <- data.frame(
    etl_source_file = rep(source_file, length(bad_indices)),
    source_row_index = bad_indices,
    reason = "column_shift_in_source",
    detected_via = "asin_format_check_failed",
    observed_asin = asin_clean[bad_indices],
    observed_sku = observed_sku,
    detected_at = Sys.time(),
    stringsAsFactors = FALSE
  )

  clean_data <- data[!bad_row, , drop = FALSE]

  if (verbose) {
    message(sprintf(
      "  [validate_amz_sales_row_integrity] %d corrupt row(s) dropped (column shift; see coverage audit)",
      length(bad_indices)
    ))
  }

  list(clean_data = clean_data, audit_rows = audit_rows)
}
