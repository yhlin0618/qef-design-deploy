#' Fix Excel serial date values in a dataframe column
#'
#' Detects Excel serial date numbers (numeric or numeric-like strings) in a
#' specified column and converts them to ISO timestamp strings
#' (\code{YYYY-MM-DD HH:MM:SS}). Values that are not recognized as Excel
#' serials (e.g. already-parsed ISO strings, text tokens) and \code{NA}s are
#' left unchanged.
#'
#' Excel serials are fractional days since \code{1899-12-30 UTC}. When
#' \code{readxl::read_excel} is called with \code{col_types = "text"}, cells
#' that Excel formats as dates are returned as numeric strings like
#' \code{"45323.435208333336"} rather than the displayed date. Downstream
#' code that does \code{CAST(col AS TIMESTAMP)} in DuckDB then fails with
#' \code{Conversion Error: invalid timestamp field format}.
#'
#' This function normalizes such columns to DuckDB-parseable ISO strings at
#' the 0IM (import) stage — canonical format enforcement per MP064.
#'
#' @param df A data.frame.
#' @param col_name Character scalar. Column name to fix. Default
#'   \code{"purchase_date"}.
#' @return The input \code{df} with \code{col_name} column normalized to
#'   character ISO timestamps where values were recognized as Excel serials,
#'   and left as-is otherwise. Column type will be \code{character}.
#'
#' @details
#' Sanity range: serial values must be in \code{[20000, 69037]}
#' (\eqn{\approx}1954-10-03..2089-01-04) to be accepted as dates. Out-of-range
#' values are kept as-is to avoid converting unrelated numeric tokens
#' (IDs, prices).
#'
#' Timezone: Excel serial origin is naive calendar date (no TZ). We assume
#' UTC for the origin (\code{1899-12-30}), matching the convention
#' documented in issue #445. Output ISO strings are TZ-naive (no \code{Z}
#' suffix), which DuckDB parses as local-time-naive timestamp.
#'
#' Locale: \code{as.numeric()} uses period as decimal separator regardless of
#' \code{LC_NUMERIC}. European-style comma-decimal strings like
#' \code{"45323,435"} become \code{NA} and are silently preserved. If
#' Excel exports from such locales are expected, pre-normalize commas
#' externally before calling this function.
#'
#' Attribute preservation: this function drops column attributes (labels,
#' classes like \code{haven_labelled}). Use only for plain character/numeric
#' columns.
#'
#' @examples
#' \dontrun{
#' df <- data.frame(purchase_date = c("45323.435208333336", "2024-11-01"))
#' fix_excel_serial_dates(df)
#' #   purchase_date
#' # 1 "2024-02-27 10:26:41"
#' # 2 "2024-11-01"
#' }
#'
#' Principles: MP064 (ETL-Derivation separation), DM_R028 (ETL data type
#' separation), MP029 (no fake data — grounded in real #436 incident).
#'
#' @export
fix_excel_serial_dates <- function(df, col_name = "purchase_date") {
  if (!col_name %in% names(df)) return(df)

  x <- df[[col_name]]
  n <- length(x)
  if (n == 0L) return(df)

  # Parse each value as numeric (suppress warnings from non-numeric tokens).
  serial_num <- suppressWarnings(as.numeric(x))

  # Sanity range for Excel dates: 1954-10-03 ≈ 20000; 2089-01-04 ≈ 69037.
  # Tight upper bound rejects unrelated numeric tokens (IDs, prices).
  # See docstring for rationale (verify-445 S1).
  is_serial <- !is.na(serial_num) &
               serial_num >= 20000 & serial_num <= 69037

  if (!any(is_serial)) return(df)

  # Build output column: start with character copy of input.
  out <- as.character(x)

  # Convert detected serials via POSIXct arithmetic.
  # 86400 = seconds per day; origin 1899-12-30 UTC.
  converted <- format(
    as.POSIXct(serial_num[is_serial] * 86400,
               origin = "1899-12-30", tz = "UTC"),
    format = "%Y-%m-%d %H:%M:%S",
    tz = "UTC"
  )
  out[is_serial] <- converted

  df[[col_name]] <- out
  df
}
