# ============================================================
# periodComparisonDefaults.R
# Period Comparison Default Settings for MAMBA
# Created: 2025-09-23
# Principle: R09 (UI-Server-Defaults Triple)
# ============================================================

#' Period Comparison Default Settings
#' 
#' @description
#' Default configuration for period comparison module.
#' 
#' @export
periodComparisonDefaults <- function() {
  list(
    # Period types configuration
    period_types = list(
      daily = list(
        label = "每日",
        format = "%Y-%m-%d",
        floor_unit = "day",
        lag_periods_yoy = 365
      ),
      weekly = list(
        label = "每週",
        format = "%Y-W%V",
        floor_unit = "week",
        lag_periods_yoy = 52
      ),
      monthly = list(
        label = "每月",
        format = "%Y-%m",
        floor_unit = "month",
        lag_periods_yoy = 12
      ),
      quarterly = list(
        label = "每季",
        format = "%Y-Q%q",
        floor_unit = "quarter",
        lag_periods_yoy = 4
      ),
      yearly = list(
        label = "每年",
        format = "%Y",
        floor_unit = "year",
        lag_periods_yoy = 1
      )
    ),
    
    # Default selections
    default_period_type = "monthly",
    default_comparison_type = "period_over_period",
    default_date_range_days = 90,
    
    # Metrics configuration
    default_metrics = c("revenue", "customers", "orders"),
    metric_labels = list(
      revenue = "營收",
      customers = "客戶數",
      orders = "訂單數",
      sales = "銷量",
      profit = "利潤",
      conversion_rate = "轉換率"
    ),
    
    # Display settings
    max_periods_display = 24,
    chart_height = "400px",
    table_page_length = 10,
    
    # Formatting
    number_format = list(
      big_mark = ",",
      decimal_mark = ".",
      digits = 0
    ),
    percent_format = list(
      digits = 1,
      suffix = "%"
    ),
    
    # Colors for trends
    trend_colors = list(
      up = "#28a745",
      down = "#dc3545",
      neutral = "#6c757d"
    ),
    
    # Chart colors
    chart_colors = c(
      "#007bff",  # Primary blue
      "#28a745",  # Success green
      "#ffc107",  # Warning yellow
      "#dc3545",  # Danger red
      "#6c757d",  # Secondary gray
      "#17a2b8"   # Info cyan
    ),
    
    # Thresholds for rate capping
    max_change_rate_display = 1.5,  # Cap at 150% for display
    
    # Database settings
    date_column_name = "date",
    
    # Localization
    language = "zh-TW",
    datatables_language_url = "//cdn.datatables.net/plug-ins/1.10.25/i18n/Chinese-traditional.json"
  )
}