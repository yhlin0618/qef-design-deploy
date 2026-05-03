#!/usr/bin/env Rscript
#' Complete Time Series with Transparency Markers
#'
#' Implements R117: Time Series Filling Transparency Rule
#'
#' This function completes missing time periods in time series data while
#' maintaining transparency about which data points are real vs filled.
#' All filled data is explicitly marked with a data_source column to
#' comply with MP029 (No Fake Data Principle).
#'
#' @param data Data frame with time series data
#' @param date_col Name of date column (default: "date")
#' @param group_cols Vector of grouping columns (e.g., c("product_line", "product_id"))
#' @param value_cols Vector of value columns to fill
#' @param fill_method Method: "forward" (last observation carried forward),
#'                   "interpolate" (linear interpolation), "zero" (zero-fill)
#' @param mark_filled Add data_source column marking REAL vs FILLED (required by R117)
#' @param warn_threshold Warning threshold for fill rate (default 0.50)
#' @param time_unit Time unit for sequence generation: "day", "week", "month" (default: "day")
#'
#' @return List with two elements:
#'   - data: Data frame with completed time series and data_source markers
#'   - fill_rate_summary: Data frame with fill rate statistics per group
#'
#' @examples
#' # Complete daily sales data with zero-fill
#' result <- fn_complete_time_series(
#'   data = sales_data,
#'   date_col = "date",
#'   group_cols = c("product_line", "country"),
#'   value_cols = c("sales", "revenue"),
#'   fill_method = "zero"
#' )
#' completed_data <- result$data
#' fill_stats <- result$fill_rate_summary
#'
#' @export
fn_complete_time_series <- function(data,
                                    date_col = "date",
                                    group_cols = c("product_line"),
                                    value_cols = c("sales", "orders"),
                                    fill_method = "forward",
                                    mark_filled = TRUE,
                                    warn_threshold = 0.50,
                                    time_unit = "day") {

  # Load required packages
  require(dplyr, quietly = TRUE)
  require(tidyr, quietly = TRUE)
  require(zoo, quietly = TRUE)

  # === VALIDATION (R117 Compliance) ===

  # CRITICAL: mark_filled must be TRUE for R117 compliance
  if (!mark_filled) {
    stop("R117 VIOLATION: mark_filled must be TRUE for transparency compliance. ",
         "Time series filling without marking violates MP029 (No Fake Data Principle).")
  }

  # Validate inputs
  if (!date_col %in% names(data)) {
    stop(sprintf("Date column '%s' not found in data", date_col))
  }

  if (!all(group_cols %in% names(data))) {
    missing_cols <- setdiff(group_cols, names(data))
    stop(sprintf("Group columns not found in data: %s",
                 paste(missing_cols, collapse = ", ")))
  }

  if (!all(value_cols %in% names(data))) {
    missing_cols <- setdiff(value_cols, names(data))
    stop(sprintf("Value columns not found in data: %s",
                 paste(missing_cols, collapse = ", ")))
  }

  # Validate fill_method
  valid_methods <- c("forward", "zero", "interpolate")
  if (!fill_method %in% valid_methods) {
    stop(sprintf("Invalid fill_method '%s'. Must be one of: %s",
                 fill_method, paste(valid_methods, collapse = ", ")))
  }

  # === STEP 1: Mark Original Data as REAL ===

  message(sprintf("[Step 1/6] Marking original data as REAL..."))
  data$data_source <- "REAL"

  original_row_count <- nrow(data)
  message(sprintf("  - Original data: %d rows", original_row_count))

  # === STEP 2: Determine Time Range ===

  message(sprintf("[Step 2/6] Determining time range..."))

  # Convert date column to Date if needed
  data[[date_col]] <- as.Date(data[[date_col]])

  min_date <- min(data[[date_col]], na.rm = TRUE)
  max_date <- max(data[[date_col]], na.rm = TRUE)

  # Create time sequence based on time_unit
  if (time_unit == "day") {
    time_sequence <- seq(min_date, max_date, by = "1 day")
  } else if (time_unit == "week") {
    time_sequence <- seq(min_date, max_date, by = "1 week")
  } else if (time_unit == "month") {
    time_sequence <- seq(min_date, max_date, by = "1 month")
  } else {
    stop(sprintf("Invalid time_unit '%s'. Must be 'day', 'week', or 'month'", time_unit))
  }

  message(sprintf("  - Time range: %s to %s", min_date, max_date))
  message(sprintf("  - Time unit: %s", time_unit))
  message(sprintf("  - Expected periods: %d", length(time_sequence)))

  # === STEP 3: Create Complete Grid ===

  message(sprintf("[Step 3/6] Creating complete time grid..."))

  # Get unique group combinations
  if (length(group_cols) > 0) {
    group_combinations <- data %>%
      select(all_of(group_cols)) %>%
      distinct()

    message(sprintf("  - Unique groups: %d", nrow(group_combinations)))

    # Create complete grid: time x groups
    complete_grid <- expand.grid(
      time = time_sequence,
      stringsAsFactors = FALSE
    ) %>%
      crossing(group_combinations)

  } else {
    # No grouping, just time sequence
    complete_grid <- data.frame(time = time_sequence)
  }

  # Rename time column to match date_col
  names(complete_grid)[names(complete_grid) == "time"] <- date_col

  expected_rows <- nrow(complete_grid)
  message(sprintf("  - Complete grid: %d rows", expected_rows))

  # === STEP 4: Join with Original Data ===

  message(sprintf("[Step 4/6] Joining with original data..."))

  # Left join to preserve all time periods
  if (length(group_cols) > 0) {
    completed_data <- complete_grid %>%
      left_join(data, by = c(date_col, group_cols))
  } else {
    completed_data <- complete_grid %>%
      left_join(data, by = date_col)
  }

  # Mark filled rows (where data_source is NA after join)
  completed_data <- completed_data %>%
    mutate(data_source = ifelse(is.na(data_source), "FILLED", data_source))

  filled_count <- sum(completed_data$data_source == "FILLED")
  message(sprintf("  - Filled rows: %d", filled_count))

  # === STEP 5: Apply Filling Method ===

  message(sprintf("[Step 5/6] Applying fill method '%s'...", fill_method))

  # Apply filling method to value columns
  if (length(group_cols) > 0) {
    completed_data <- completed_data %>%
      group_by(across(all_of(group_cols))) %>%
      arrange(across(all_of(c(group_cols, date_col))))
  } else {
    completed_data <- completed_data %>%
      arrange(!!sym(date_col))
  }

  for (val_col in value_cols) {
    if (fill_method == "forward") {
      # Forward fill (last observation carried forward)
      completed_data <- completed_data %>%
        fill(!!sym(val_col), .direction = "down")

    } else if (fill_method == "zero") {
      # Zero-fill (most common for sales data)
      completed_data <- completed_data %>%
        mutate(!!sym(val_col) := ifelse(
          data_source == "FILLED" & is.na(!!sym(val_col)),
          0,
          !!sym(val_col)
        ))

    } else if (fill_method == "interpolate") {
      # Linear interpolation
      completed_data <- completed_data %>%
        mutate(!!sym(val_col) := zoo::na.approx(
          !!sym(val_col),
          na.rm = FALSE
        ))
    }
  }

  if (length(group_cols) > 0) {
    completed_data <- completed_data %>% ungroup()
  }

  # === STEP 6: Calculate Fill Rate Metadata ===

  message(sprintf("[Step 6/6] Calculating fill rate metadata..."))

  if (length(group_cols) > 0) {
    fill_rate_summary <- completed_data %>%
      group_by(across(all_of(group_cols))) %>%
      summarise(
        total_rows = n(),
        real_rows = sum(data_source == "REAL"),
        filled_rows = sum(data_source == "FILLED"),
        fill_rate = filled_rows / total_rows,
        .groups = "drop"
      ) %>%
      arrange(desc(fill_rate))
  } else {
    fill_rate_summary <- completed_data %>%
      summarise(
        group_name = "all_data",
        total_rows = n(),
        real_rows = sum(data_source == "REAL"),
        filled_rows = sum(data_source == "FILLED"),
        fill_rate = filled_rows / total_rows
      )
  }

  overall_fill_rate <- sum(completed_data$data_source == "FILLED") / nrow(completed_data)

  message(sprintf("  ✓ Overall fill rate: %.1f%%", overall_fill_rate * 100))

  # === R117 WARNING: Alert if High Fill Rate ===

  high_fill_groups <- fill_rate_summary %>%
    filter(fill_rate > warn_threshold)

  if (nrow(high_fill_groups) > 0) {
    warning(sprintf(
      "\n╔══════════════════════════════════════════════════════════════╗\n",
      "║ R117 WARNING: High Fill Rate Detected                       ║\n",
      "╠══════════════════════════════════════════════════════════════╣\n",
      "║ %d groups have fill_rate > %.0f%%                            ║\n",
      "║ This may indicate data quality issues:                      ║\n",
      "║   - Product not actually active in this market?             ║\n",
      "║   - Data collection failing?                                ║\n",
      "║   - Should this group be excluded from analysis?            ║\n",
      "╚══════════════════════════════════════════════════════════════╝",
      nrow(high_fill_groups),
      warn_threshold * 100
    ))

    # Print top 10 high-fill groups
    print(head(high_fill_groups, 10))
  }

  # === Add Metadata Columns (R117 Requirement) ===

  completed_data$filling_method <- fill_method
  completed_data$filling_timestamp <- Sys.time()

  # === FINAL SUMMARY ===

  message("\n═══════════════════════════════════════════════════════════")
  message(sprintf("✅ R117: Time Series Completion Complete"))
  message("═══════════════════════════════════════════════════════════")
  message(sprintf("  Original rows:    %d", original_row_count))
  message(sprintf("  Completed rows:   %d", nrow(completed_data)))
  message(sprintf("  Real data:        %d (%.1f%%)",
                  sum(completed_data$data_source == "REAL"),
                  (sum(completed_data$data_source == "REAL") / nrow(completed_data)) * 100))
  message(sprintf("  Filled data:      %d (%.1f%%)",
                  sum(completed_data$data_source == "FILLED"),
                  overall_fill_rate * 100))
  message(sprintf("  Filling method:   %s", fill_method))
  message(sprintf("  Groups analyzed:  %d", nrow(fill_rate_summary)))
  message("═══════════════════════════════════════════════════════════\n")

  # Return list with data and metadata
  return(list(
    data = completed_data,
    fill_rate_summary = fill_rate_summary
  ))
}
