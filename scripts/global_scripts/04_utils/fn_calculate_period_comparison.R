# ============================================================
# fn_calculate_period_comparison.R
# Period Comparison Calculation Utilities
# Created: 2025-09-23
# Principles: MP30 (Vectorization), R49 (Apply Over Loops)
# ============================================================

#' Calculate Period-over-Period Comparison
#'
#' @param data Data frame with time series data
#' @param date_col Name of date column
#' @param value_cols Character vector of value columns to compare
#' @param period_type Type of period aggregation (daily, weekly, monthly, quarterly, yearly)
#' @param comparison_type Type of comparison (period_over_period, year_over_year, custom)
#' @param periods_back Number of periods to look back (for custom comparison)
#' @param max_rate Maximum rate to cap (default 1.5 = 150%)
#'
#' @return Data frame with comparison metrics added
#'
#' @description
#' Calculates period-over-period comparisons with difference and rate calculations.
#' Uses vectorized operations for efficiency (MP30).
#'
#' @export
calculate_period_comparison <- function(data,
                                       date_col = "date",
                                       value_cols = c("revenue", "customers", "orders"),
                                       period_type = "monthly",
                                       comparison_type = "period_over_period",
                                       periods_back = 1,
                                       max_rate = 1.5) {

  # Validate inputs
  if (!date_col %in% names(data)) {
    stop("Date column '", date_col, "' not found in data")
  }

  # Check for required packages
  if (!requireNamespace("lubridate", quietly = TRUE)) {
    stop("Package 'lubridate' is required for date operations")
  }

  # Convert date column to Date type if needed
  if (!inherits(data[[date_col]], "Date")) {
    data[[date_col]] <- as.Date(data[[date_col]])
  }

  # Aggregate by period type
  data_aggregated <- aggregate_by_period(data, date_col, value_cols, period_type)

  # Determine lag periods based on comparison type
  if (comparison_type == "period_over_period") {
    lag_n <- 1
  } else if (comparison_type == "year_over_year") {
    lag_n <- get_yoy_lag_periods(period_type)
  } else {
    lag_n <- periods_back
  }

  # Calculate comparisons for each value column (vectorized)
  result <- data_aggregated

  for (col in value_cols) {
    if (col %in% names(data_aggregated)) {
      # Previous period value
      prev_col <- paste0(col, "_prev")
      result[[prev_col]] <- lag(result[[col]], n = lag_n)

      # Difference
      diff_col <- paste0(col, "_diff")
      result[[diff_col]] <- result[[col]] - result[[prev_col]]

      # Rate of change (with zero division handling)
      rate_col <- paste0(col, "_rate")
      result[[rate_col]] <- ifelse(
        result[[prev_col]] == 0 | is.na(result[[prev_col]]),
        NA,
        pmin(
          (result[[col]] - result[[prev_col]]) / result[[prev_col]],
          max_rate
        )
      )

      # Add trend indicator
      trend_col <- paste0(col, "_trend")
      result[[trend_col]] <- case_when(
        is.na(result[[rate_col]]) ~ "neutral",
        result[[rate_col]] > 0.05 ~ "up",
        result[[rate_col]] < -0.05 ~ "down",
        TRUE ~ "neutral"
      )
    }
  }

  result
}

#' Aggregate Data by Period Type
#'
#' @param data Data frame to aggregate
#' @param date_col Name of date column
#' @param value_cols Columns to aggregate
#' @param period_type Period type for aggregation
#'
#' @return Aggregated data frame
#'
#' @keywords internal
aggregate_by_period <- function(data, date_col, value_cols, period_type) {

  # Create period column based on type
  period_col <- "period"

  if (period_type == "daily") {
    data[[period_col]] <- as.Date(data[[date_col]])
  } else if (period_type == "weekly") {
    data[[period_col]] <- lubridate::floor_date(data[[date_col]], "week")
  } else if (period_type == "monthly") {
    data[[period_col]] <- lubridate::floor_date(data[[date_col]], "month")
  } else if (period_type == "quarterly") {
    data[[period_col]] <- lubridate::floor_date(data[[date_col]], "quarter")
  } else if (period_type == "yearly") {
    data[[period_col]] <- lubridate::floor_date(data[[date_col]], "year")
  } else {
    stop("Invalid period_type: ", period_type)
  }

  # Aggregate values
  agg_formula <- reformulate(period_col, response = ".")

  # Build aggregation list
  agg_list <- list()
  for (col in value_cols) {
    if (col %in% names(data)) {
      agg_list[[col]] <- as.formula(paste0(col, " ~ sum(", col, ", na.rm = TRUE)"))
    }
  }

  # Perform aggregation
  result <- data %>%
    group_by(!!sym(period_col)) %>%
    summarise(
      across(all_of(intersect(value_cols, names(data))),
             list(sum = ~sum(., na.rm = TRUE),
                  avg = ~mean(., na.rm = TRUE),
                  count = ~n()),
             .names = "{.col}_{.fn}"),
      .groups = "drop"
    ) %>%
    arrange(!!sym(period_col))

  # Rename sum columns to original names
  for (col in value_cols) {
    sum_col <- paste0(col, "_sum")
    if (sum_col %in% names(result)) {
      names(result)[names(result) == sum_col] <- col
    }
  }

  result
}

#' Get Year-over-Year Lag Periods
#'
#' @param period_type Period type
#' @return Number of periods for YoY comparison
#'
#' @keywords internal
get_yoy_lag_periods <- function(period_type) {
  switch(period_type,
    "daily" = 365,
    "weekly" = 52,
    "monthly" = 12,
    "quarterly" = 4,
    "yearly" = 1,
    stop("Invalid period_type: ", period_type)
  )
}

#' Format Period Label
#'
#' @param date Date or period to format
#' @param period_type Type of period
#' @return Formatted period label
#'
#' @export
format_period_label <- function(date, period_type) {
  if (is.na(date)) return(NA)

  switch(period_type,
    "daily" = format(date, "%Y-%m-%d"),
    "weekly" = format(date, "%Y-W%V"),
    "monthly" = format(date, "%Y-%m"),
    "quarterly" = paste0(format(date, "%Y"), "-Q", lubridate::quarter(date)),
    "yearly" = format(date, "%Y"),
    as.character(date)
  )
}

#' Calculate Rolling Window Metrics
#'
#' @param data Data frame with time series
#' @param value_col Column to calculate rolling metrics for
#' @param window_size Size of rolling window
#' @param fun Function to apply (mean, sum, etc.)
#'
#' @return Vector of rolling metrics
#'
#' @export
calculate_rolling_metrics <- function(data, value_col, window_size = 7, fun = mean) {
  if (!value_col %in% names(data)) {
    stop("Column '", value_col, "' not found in data")
  }

  # Use zoo::rollapply for rolling calculations
  if (requireNamespace("zoo", quietly = TRUE)) {
    zoo::rollapply(data[[value_col]],
                   width = window_size,
                   FUN = fun,
                   na.rm = TRUE,
                   fill = NA,
                   align = "right")
  } else {
    # Fallback to base R implementation
    sapply(seq_along(data[[value_col]]), function(i) {
      if (i < window_size) {
        NA
      } else {
        fun(data[[value_col]][(i - window_size + 1):i], na.rm = TRUE)
      }
    })
  }
}