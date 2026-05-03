# =============================================================================
# SCHEMA_004: Time Series Sales Table Definition
# =============================================================================
# Purpose: Define the standard schema for time series sales data
# Used by: WISER, MAMBA applications for time series analysis
# Critical: This table feeds into Poisson regression models
# =============================================================================

# Schema Definition
SCHEMA_time_series_sales <- list(

  table_name = "df_{platform}_sales_complete_time_series_{product_line}",

  description = "Stores enriched time series sales data with temporal features and product attributes",

  columns = list(

    # =========================================================================
    # IDENTIFIER COLUMNS (Required)
    # =========================================================================

    eby_item_id = list(
      type = "VARCHAR",
      required = FALSE,
      description = "eBay item identifier",
      example = "EBY_123456"
    ),

    cbz_item_id = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Primary item identifier",
      example = "CBZ_789012"
    ),

    product_line_id = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Product line identifier (with .x/.y suffix in enriched tables)",
      example = "alf"
    ),

    # =========================================================================
    # TIME COLUMNS (Required)
    # =========================================================================

    time = list(
      type = "DATE",
      required = TRUE,
      description = "Date of the sales record",
      example = "2025-01-15"
    ),

    year = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Year component",
      example = 2025
    ),

    day = list(
      type = "INTEGER",
      required = TRUE,
      description = "Day of month (1-31)",
      example = 15
    ),

    # =========================================================================
    # MONTH INDICATORS (One-hot encoded)
    # =========================================================================

    month_1 = list(
      type = "DOUBLE",
      required = TRUE,
      description = "January indicator (1 or 0)",
      example = 1
    ),

    month_2 = list(
      type = "DOUBLE",
      required = TRUE,
      description = "February indicator",
      example = 0
    ),

    month_3 = list(
      type = "DOUBLE",
      required = TRUE,
      description = "March indicator",
      example = 0
    ),

    month_4 = list(
      type = "DOUBLE",
      required = TRUE,
      description = "April indicator",
      example = 0
    ),

    month_5 = list(
      type = "DOUBLE",
      required = TRUE,
      description = "May indicator",
      example = 0
    ),

    month_6 = list(
      type = "DOUBLE",
      required = TRUE,
      description = "June indicator",
      example = 0
    ),

    month_7 = list(
      type = "DOUBLE",
      required = TRUE,
      description = "July indicator",
      example = 0
    ),

    month_8 = list(
      type = "DOUBLE",
      required = TRUE,
      description = "August indicator",
      example = 0
    ),

    month_9 = list(
      type = "DOUBLE",
      required = TRUE,
      description = "September indicator",
      example = 0
    ),

    month_10 = list(
      type = "DOUBLE",
      required = TRUE,
      description = "October indicator",
      example = 0
    ),

    month_11 = list(
      type = "DOUBLE",
      required = TRUE,
      description = "November indicator",
      example = 0
    ),

    month_12 = list(
      type = "DOUBLE",
      required = TRUE,
      description = "December indicator",
      example = 0
    ),

    # =========================================================================
    # WEEKDAY INDICATORS (One-hot encoded)
    # =========================================================================

    monday = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Monday indicator (1 or 0)",
      example = 0
    ),

    tuesday = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Tuesday indicator",
      example = 0
    ),

    wednesday = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Wednesday indicator",
      example = 1
    ),

    thursday = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Thursday indicator",
      example = 0
    ),

    friday = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Friday indicator",
      example = 0
    ),

    saturday = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Saturday indicator",
      example = 0
    ),

    sunday = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Sunday indicator",
      example = 0
    ),

    # =========================================================================
    # SALES DATA (Required)
    # =========================================================================

    sales = list(
      type = "INTEGER",
      required = TRUE,
      description = "Sales count/volume for the time period",
      example = 25,
      critical = "Target variable for Poisson regression"
    ),

    sales_platform = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Platform-specific sales metric",
      example = 1.0
    ),

    # =========================================================================
    # PRODUCT ATTRIBUTES (Enriched tables)
    # =========================================================================

    url = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Product URL",
      example = "https://example.com/product/123"
    ),

    product_name = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Product name",
      example = "Turbo Compressor Wheel ALF-01"
    ),

    seller_name = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Seller name",
      example = "TurboParts Inc"
    ),

    brand = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Product brand (may have .x/.y suffix)",
      example = "MAMBA"
    ),

    manufacturer = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Product manufacturer",
      example = "MAMBA Manufacturing"
    ),

    # =========================================================================
    # PRICING DATA
    # =========================================================================

    original_price_converted_to_usd = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Original price in USD",
      example = 299.99
    ),

    discounted_price_converted_to_usd = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Discounted price in USD",
      example = 249.99
    ),

    # =========================================================================
    # COMMENT PROPERTY RATINGS (Optional - in enriched tables)
    # =========================================================================
    # Note: These are the same as in df_position but included in time series

    rating = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Overall product rating",
      example = 4.5
    ),

    # Chinese comment properties (same as SCHEMA_002)
    # Not listing all 33 properties here for brevity, but they follow same pattern

    # =========================================================================
    # MISSING INDICATORS (Enriched tables)
    # =========================================================================
    # For each nullable field, there's a corresponding _is_missing indicator

    rating_is_missing = list(
      type = "INTEGER",
      required = FALSE,
      description = "Indicator for missing rating (1 = missing, 0 = present)",
      example = 0
    ),

    # =========================================================================
    # PROCESSING METADATA
    # =========================================================================

    processed_date = list(
      type = "DATE",
      required = FALSE,
      description = "Date when data was processed",
      example = "2025-01-15"
    ),

    processing_version = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Version of processing pipeline",
      example = "v2.1"
    ),

    enriched_date = list(
      type = "DATE",
      required = FALSE,
      description = "Date when data was enriched",
      example = "2025-01-16"
    ),

    enrichment_version = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Version of enrichment pipeline",
      example = "v1.5"
    ),

    cleansed_date = list(
      type = "DATE",
      required = FALSE,
      description = "Date when data was cleansed",
      example = "2025-01-14"
    ),

    source_table = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Source table name",
      example = "raw_sales_cbz"
    )
  ),

  # =========================================================================
  # TABLE CONSTRAINTS
  # =========================================================================

  constraints = list(
    primary_key = c("cbz_item_id", "time"),

    indexes = c("time", "product_line_id", "sales"),

    time_constraints = list(
      month_sum = "Sum of month indicators must equal 1",
      weekday_sum = "Sum of weekday indicators must equal 1"
    )
  ),

  # =========================================================================
  # USAGE EXAMPLES
  # =========================================================================

  examples = list(

    aggregate_by_month = "
    -- Aggregate sales by month
    SELECT
      year,
      CASE
        WHEN month_1 = 1 THEN 1
        WHEN month_2 = 1 THEN 2
        WHEN month_3 = 1 THEN 3
        WHEN month_4 = 1 THEN 4
        WHEN month_5 = 1 THEN 5
        WHEN month_6 = 1 THEN 6
        WHEN month_7 = 1 THEN 7
        WHEN month_8 = 1 THEN 8
        WHEN month_9 = 1 THEN 9
        WHEN month_10 = 1 THEN 10
        WHEN month_11 = 1 THEN 11
        WHEN month_12 = 1 THEN 12
      END as month,
      SUM(sales) as total_sales,
      COUNT(DISTINCT cbz_item_id) as unique_products
    FROM df_cbz_sales_complete_time_series_alf
    GROUP BY year, month
    ORDER BY year, month
    ",

    prepare_for_poisson = "
    -- Prepare data for Poisson regression
    SELECT
      time,
      sales,
      year,
      day,
      month_1, month_2, month_3, month_4, month_5, month_6,
      month_7, month_8, month_9, month_10, month_11, month_12,
      monday, tuesday, wednesday, thursday, friday, saturday, sunday,
      LOG(NULLIF(discounted_price_converted_to_usd, 0)) as log_price,
      rating,
      品質優良,
      性能卓越
    FROM df_cbz_sales_complete_time_series_alf
    WHERE sales IS NOT NULL
      AND sales >= 0
    "
  )
)

# =============================================================================
# VALIDATION FUNCTION
# =============================================================================

validate_time_series_table <- function(con, table_name) {

  if (!dbExistsTable(con, table_name)) {
    return(list(
      valid = FALSE,
      error = paste("Table", table_name, "does not exist")
    ))
  }

  # Check required columns
  actual_cols <- dbListFields(con, table_name)

  # Core required columns for basic time series
  required_core <- c("time", "sales", "year", "day")

  # Check month indicators
  month_cols <- paste0("month_", 1:12)

  # Check weekday indicators
  weekday_cols <- c("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")

  missing_core <- setdiff(required_core, actual_cols)
  missing_months <- setdiff(month_cols, actual_cols)
  missing_weekdays <- setdiff(weekday_cols, actual_cols)

  errors <- c()
  if (length(missing_core) > 0) {
    errors <- c(errors, paste("Missing core columns:", paste(missing_core, collapse = ", ")))
  }
  if (length(missing_months) > 0) {
    errors <- c(errors, paste("Missing month indicators:", paste(missing_months, collapse = ", ")))
  }
  if (length(missing_weekdays) > 0) {
    errors <- c(errors, paste("Missing weekday indicators:", paste(missing_weekdays, collapse = ", ")))
  }

  if (length(errors) > 0) {
    return(list(
      valid = FALSE,
      errors = errors
    ))
  }

  # Check data quality
  quality_check <- dbGetQuery(con, paste0("
    SELECT
      COUNT(*) as total_rows,
      SUM(CASE WHEN sales < 0 THEN 1 ELSE 0 END) as negative_sales,
      SUM(CASE WHEN sales IS NULL THEN 1 ELSE 0 END) as null_sales,
      MIN(time) as min_date,
      MAX(time) as max_date
    FROM ", table_name))

  return(list(
    valid = TRUE,
    message = "Validation passed",
    statistics = quality_check
  ))
}

# =============================================================================
# ETL HELPER FUNCTIONS
# =============================================================================

create_time_features <- function(df) {
  # Add time-based features for analysis

  df %>%
    mutate(
      year = year(time),
      month = month(time),
      day = day(time),
      weekday = weekdays(time),

      # Create one-hot encoded month columns
      month_1 = ifelse(month == 1, 1, 0),
      month_2 = ifelse(month == 2, 1, 0),
      month_3 = ifelse(month == 3, 1, 0),
      month_4 = ifelse(month == 4, 1, 0),
      month_5 = ifelse(month == 5, 1, 0),
      month_6 = ifelse(month == 6, 1, 0),
      month_7 = ifelse(month == 7, 1, 0),
      month_8 = ifelse(month == 8, 1, 0),
      month_9 = ifelse(month == 9, 1, 0),
      month_10 = ifelse(month == 10, 1, 0),
      month_11 = ifelse(month == 11, 1, 0),
      month_12 = ifelse(month == 12, 1, 0),

      # Create one-hot encoded weekday columns
      monday = ifelse(weekday == "Monday", 1, 0),
      tuesday = ifelse(weekday == "Tuesday", 1, 0),
      wednesday = ifelse(weekday == "Wednesday", 1, 0),
      thursday = ifelse(weekday == "Thursday", 1, 0),
      friday = ifelse(weekday == "Friday", 1, 0),
      saturday = ifelse(weekday == "Saturday", 1, 0),
      sunday = ifelse(weekday == "Sunday", 1, 0)
    ) %>%
    select(-month, -weekday)  # Remove intermediate columns
}