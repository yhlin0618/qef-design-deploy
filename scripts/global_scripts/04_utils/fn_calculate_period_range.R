################################################################################
# Calculate Date Range for Standard Period Types
################################################################################
#
# PRINCIPLE COMPLIANCE:
#   - MP135: Pre-Computed Analytics Principle (standard period calculations)
#   - DM_R120: ETL Period Standardization (standardized period patterns)
#
# PURPOSE:
#   Provides consistent date range calculations for pre-computed analytics.
#   Used by DRV scripts to compute consistent date ranges across the system.
#
# DESIGN RATIONALE:
#   - Single source of truth for period calculations
#   - Reference date should be latest order_date from data, NOT Sys.Date()
#   - Ensures all DRV scripts use identical period logic
#   - Prevents period calculation drift between different analytics
#
################################################################################

#' Calculate Date Range for Standard Period Types
#'
#' @description
#' Implements MP135 (Pre-Computed Analytics Principle) standard period calculations.
#' Used by DRV scripts to compute consistent date ranges for pre-computed analytics.
#'
#' @param period_type Character. Standard period identifier:
#'   - Rolling windows: "rolling_30d", "rolling_90d", "rolling_180d", "rolling_365d"
#'   - Fixed windows: "month_{YYYY_MM}", "quarter_{YYYY_Q#}", "year_{YYYY}"
#'   - Special: "all_time"
#' @param reference_date Date. Reference date for calculations (default: latest data date from caller)
#'   Should be the latest order_date in your transaction data, NOT Sys.Date()
#' @param all_time_start_date Date. System start date for "all_time" calculations (default: 2020-01-01)
#'
#' @return List with two elements:
#'   - start: Date object representing period start
#'   - end: Date object representing period end
#'
#' @examples
#' # Rolling window from latest data date
#' latest_date <- as.Date("2024-11-13")
#' calculate_period_range("rolling_90d", reference_date = latest_date)
#' # Returns: list(start = as.Date("2024-08-15"), end = as.Date("2024-11-13"))
#'
#' # Fixed calendar month
#' calculate_period_range("month_2024_11", reference_date = latest_date)
#' # Returns: list(start = as.Date("2024-11-01"), end = as.Date("2024-11-30"))
#'
#' # All time
#' calculate_period_range("all_time", reference_date = latest_date)
#' # Returns: list(start = as.Date("2020-01-01"), end = as.Date("2024-11-13"))
#'
#' @export
calculate_period_range <- function(period_type,
                                    reference_date = Sys.Date(),
                                    all_time_start_date = as.Date("2020-01-01")) {

  # MP135: Validate inputs for data quality assurance
  if (!inherits(reference_date, "Date")) {
    stop("reference_date must be a Date object")
  }

  if (!is.character(period_type) || length(period_type) != 1) {
    stop("period_type must be a single character string")
  }

  # DM_R120: Calculate date range based on standard period types
  result <- switch(period_type,

    # Rolling windows (from reference_date backwards)
    "rolling_30d" = list(
      start = reference_date - 30,
      end = reference_date
    ),

    "rolling_90d" = list(
      start = reference_date - 90,
      end = reference_date
    ),

    "rolling_180d" = list(
      start = reference_date - 180,
      end = reference_date
    ),

    "rolling_365d" = list(
      start = reference_date - 365,
      end = reference_date
    ),

    # All time
    "all_time" = list(
      start = all_time_start_date,
      end = reference_date
    ),

    # Pattern matching for dynamic periods
    {
      # If pattern matches month_YYYY_MM
      if (grepl("^month_\\d{4}_\\d{2}$", period_type)) {
        year_month <- gsub("month_", "", period_type)
        year <- as.integer(substr(year_month, 1, 4))
        month <- as.integer(substr(year_month, 6, 7))

        # Validate month
        if (month < 1 || month > 12) {
          stop(sprintf("Invalid month in period_type: %s (month must be 01-12)", period_type))
        }

        start_date <- as.Date(sprintf("%04d-%02d-01", year, month))

        # Last day of month
        if (month == 12) {
          end_date <- as.Date(sprintf("%04d-01-01", year + 1)) - 1
        } else {
          end_date <- as.Date(sprintf("%04d-%02d-01", year, month + 1)) - 1
        }

        list(start = start_date, end = end_date)

      } else if (grepl("^quarter_\\d{4}_Q[1-4]$", period_type)) {
        # If pattern matches quarter_YYYY_Q#
        year_quarter <- gsub("quarter_", "", period_type)
        year <- as.integer(substr(year_quarter, 1, 4))
        quarter <- as.integer(substr(year_quarter, 7, 7))

        start_month <- (quarter - 1) * 3 + 1
        start_date <- as.Date(sprintf("%04d-%02d-01", year, start_month))

        # End date calculation: last day of the quarter's final month
        end_month <- start_month + 2
        if (end_month == 12) {
          # December is last month - use Dec 31
          end_date <- as.Date(sprintf("%04d-12-31", year))
        } else if (end_month > 12) {
          # Should not happen with valid quarters, but handle gracefully
          end_date <- as.Date(sprintf("%04d-12-31", year))
        } else {
          # Normal case: get first day of next month and subtract 1
          end_date <- as.Date(sprintf("%04d-%02d-01", year, end_month + 1)) - 1
        }

        list(start = start_date, end = end_date)

      } else if (grepl("^year_\\d{4}$", period_type)) {
        # If pattern matches year_YYYY
        year <- as.integer(gsub("year_", "", period_type))

        list(
          start = as.Date(sprintf("%04d-01-01", year)),
          end = as.Date(sprintf("%04d-12-31", year))
        )

      } else {
        # Unknown period type
        stop(sprintf(
          "Unknown period_type: %s. Valid types:\n  - Rolling: rolling_30d, rolling_90d, rolling_180d, rolling_365d\n  - Fixed: month_YYYY_MM, quarter_YYYY_Q#, year_YYYY\n  - Special: all_time",
          period_type
        ))
      }
    }
  )

  # MP135: Validate result for data quality
  if (result$start > result$end) {
    stop(sprintf("Invalid date range: start (%s) is after end (%s)",
                 result$start, result$end))
  }

  return(result)
}
