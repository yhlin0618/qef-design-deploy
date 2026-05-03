# fn_import_adapter_gsheets.R - Generic Google Sheets Import Adapter
# ==============================================================================
# Following MP064: ETL-Derivation Separation (adapter = pure data reading)
# Following DM_R037: Source-Type + Version ETL naming support
# Following SO_R007: One Function One File
#
# Adapter responsibility: Open sheet -> Read content -> Standardize column names
# NOT responsible for: Business logic, DB writes, validation
# ==============================================================================

#' Import Data from Google Sheets
#'
#' Generic adapter for reading a Google Sheet into a data.frame.
#' Requires the googlesheets4 package and proper authentication.
#'
#' @param sheet_id Character. The Google Sheets ID (the long string in the URL).
#' @param sheet_name Character or integer. Sheet tab to read (default 1).
#' @param column_mapping Named character vector (optional). Maps source column
#'   names to target names.
#' @param range Character (optional). Cell range to read, e.g. "A1:G100".
#' @param max_retries Integer. Number of retries on timeout (default 3).
#' @param retry_delay Numeric. Seconds to wait between retries (default 5).
#' @param verbose Logical. Print progress messages (default TRUE).
#'
#' @return A \code{data.frame}. Returns an empty data.frame (0 rows) on failure
#'   after all retries.
import_from_gsheets <- function(sheet_id,
                                sheet_name = 1,
                                column_mapping = NULL,
                                range = NULL,
                                max_retries = 3,
                                retry_delay = 5,
                                verbose = TRUE) {

  if (!requireNamespace("googlesheets4", quietly = TRUE)) {
    stop("Package 'googlesheets4' is required. Install with: install.packages('googlesheets4')")
  }

  # Retry loop for transient Google API errors ----------------------------------
  df <- NULL
  for (attempt in seq_len(max_retries)) {
    tryCatch({
      if (verbose) {
        message(sprintf("import_from_gsheets: Attempt %d/%d - Reading sheet '%s'",
                        attempt, max_retries, sheet_name))
      }

      df <- as.data.frame(
        googlesheets4::read_sheet(
          ss = sheet_id,
          sheet = sheet_name,
          range = range
        )
      )
      break

    }, error = function(e) {
      if (attempt < max_retries && grepl("Timeout|429|RESOURCE_EXHAUSTED",
                                          e$message, ignore.case = TRUE)) {
        if (verbose) {
          message(sprintf("  Timeout/rate-limit on attempt %d, retrying in %ds...",
                          attempt, retry_delay))
        }
        Sys.sleep(retry_delay)
      } else {
        warning("import_from_gsheets: Failed after ", attempt, " attempt(s): ",
                e$message)
      }
    })
  }

  if (is.null(df) || nrow(df) == 0) {
    if (verbose) message("import_from_gsheets: No data retrieved")
    return(data.frame())
  }

  # Standardise column names to snake_case --------------------------------------
  names(df) <- tolower(names(df))
  names(df) <- gsub("[- ]+", "_", names(df))
  names(df) <- gsub("[^a-z0-9_\u4e00-\u9fff]", "", names(df))

  # Apply column mapping
  if (!is.null(column_mapping)) {
    for (target in names(column_mapping)) {
      src <- column_mapping[[target]]
      if (src %in% names(df)) {
        names(df)[names(df) == src] <- target
      }
    }
  }

  if (verbose) {
    message("import_from_gsheets: Read ", nrow(df), " rows, ",
            ncol(df), " columns")
  }

  df
}
