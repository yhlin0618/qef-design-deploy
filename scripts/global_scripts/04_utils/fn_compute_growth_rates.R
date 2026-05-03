#' @title Compute MoM / YoY / QoQ growth rates on a monthly time series
#' @description Pure helper that takes a data.frame with `year_month` (character
#'   "YYYY-MM") and a numeric value column, returns the same data.frame with
#'   three new columns: `mom_pct`, `yoy_pct`, `qoq_pct` (in percent).
#'
#'   Formulas (all return % change vs prior period; NA when prior is missing
#'   or 0 to avoid divide-by-zero):
#'     - mom_pct: month j vs month j-1 (lag 1)
#'     - yoy_pct: month j vs same month one year ago (lag 12, same month name)
#'     - qoq_pct: month j vs same month one quarter ago (lag 3)
#'
#'   Lag-3 QoQ keeps a unique value per month (parallel to YoY's lag-12)
#'   rather than calendar-quarter sums (which would repeat across 3 months).
#'   See plan tier discussion in issue #416.
#'
#' @param df data.frame with at least `year_month` (character "YYYY-MM") and
#'   `value_col` numeric. Order does not matter — function sorts internally.
#' @param value_col Character. Name of revenue / metric column. Default
#'   `"total_revenue"`.
#' @param time_col Character. Name of year_month column. Default `"year_month"`.
#' @return data.frame with original columns plus `mom_pct`, `yoy_pct`,
#'   `qoq_pct`. Row order matches input.
#' @principle MP029 (no fake data), DEV_R001 (vectorized), DEV_R052 (English keys)

compute_growth_rates <- function(df,
                                 value_col = "total_revenue",
                                 time_col = "year_month") {
  if (!is.data.frame(df)) {
    stop("compute_growth_rates: df must be a data.frame")
  }
  if (!time_col %in% names(df)) {
    stop(sprintf("compute_growth_rates: time_col '%s' not in df", time_col))
  }
  if (!value_col %in% names(df)) {
    stop(sprintf("compute_growth_rates: value_col '%s' not in df", value_col))
  }

  # Preserve original row order via index
  orig_idx <- seq_len(nrow(df))
  df$.__orig_idx <- orig_idx

  # Sort by time_col ascending for lookback
  ord <- order(df[[time_col]])
  df_sorted <- df[ord, , drop = FALSE]

  ym_vec <- df_sorted[[time_col]]
  val_vec <- df_sorted[[value_col]]
  n <- nrow(df_sorted)

  mom <- rep(NA_real_, n)
  yoy <- rep(NA_real_, n)
  qoq <- rep(NA_real_, n)

  # Build lookup index: ym -> row position (handles non-contiguous months)
  ym_to_idx <- setNames(seq_len(n), ym_vec)

  shift_ym <- function(ym, months_back) {
    # ym = "YYYY-MM"; returns shifted "YYYY-MM" or NA on bad input
    if (is.na(ym) || !nzchar(ym)) return(NA_character_)
    parts <- strsplit(ym, "-", fixed = TRUE)[[1]]
    if (length(parts) != 2L) return(NA_character_)
    yr <- suppressWarnings(as.integer(parts[1]))
    mo <- suppressWarnings(as.integer(parts[2]))
    if (is.na(yr) || is.na(mo)) return(NA_character_)
    total_months <- yr * 12L + (mo - 1L) - months_back
    new_yr <- total_months %/% 12L
    new_mo <- (total_months %% 12L) + 1L
    sprintf("%04d-%02d", new_yr, new_mo)
  }

  lookup_pct <- function(j, lag) {
    cur <- val_vec[j]
    if (is.na(cur)) return(NA_real_)
    prev_ym <- shift_ym(ym_vec[j], lag)
    if (is.na(prev_ym) || !(prev_ym %in% names(ym_to_idx))) return(NA_real_)
    pidx <- ym_to_idx[[prev_ym]]
    pval <- val_vec[pidx]
    if (is.na(pval) || pval <= 0) return(NA_real_)
    (cur - pval) / pval * 100
  }

  for (j in seq_len(n)) {
    mom[j] <- lookup_pct(j, 1L)
    qoq[j] <- lookup_pct(j, 3L)
    yoy[j] <- lookup_pct(j, 12L)
  }

  df_sorted$mom_pct <- mom
  df_sorted$yoy_pct <- yoy
  df_sorted$qoq_pct <- qoq

  # Restore original order
  df_out <- df_sorted[order(df_sorted$.__orig_idx), , drop = FALSE]
  df_out$.__orig_idx <- NULL
  rownames(df_out) <- NULL
  df_out
}
