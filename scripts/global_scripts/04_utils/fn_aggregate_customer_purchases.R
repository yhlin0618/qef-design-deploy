#' Aggregate Customer Purchases by Month
#'
#' Aggregates customer purchase data by month, calculating total spending
#' and purchase frequency for each customer within each month period.
#' Follows MAMBA principles: R021 (one function one file), R067 (functional encapsulation),
#' and MP080 (aggregate variable naming).
#'
#' @param purchase_data Data frame containing customer purchase records with columns:
#'   - customer_id: Unique customer identifier (character or numeric)
#'   - purchase_date: Date of purchase (Date or POSIXct)
#'   - purchase_amount: Amount spent (numeric)
#'   - order_id: Optional unique order identifier (character or numeric)
#' @param date_column Character string specifying the name of the date column 
#'   (default: "purchase_date")
#' @param amount_column Character string specifying the name of the amount column
#'   (default: "purchase_amount")
#' @param customer_column Character string specifying the name of the customer ID column
#'   (default: "customer_id")
#' @param include_quarters Logical indicating whether to include quarterly aggregations
#'   (default: FALSE)
#' @param fiscal_year_start Integer indicating the starting month of fiscal year
#'   (1-12, default: 1 for January)
#'
#' @return A data frame with columns:
#'   - customer_id: Customer identifier
#'   - year_month: Year-month in format YYYY-MM
#'   - total_spending_monthly: Total amount spent by customer in the month
#'   - purchase_frequency_monthly: Number of purchases made in the month
#'   - average_purchase_amount_monthly: Average purchase amount for the month
#'   - first_purchase_date_monthly: Date of first purchase in the month
#'   - last_purchase_date_monthly: Date of last purchase in the month
#'   - days_between_first_last_monthly: Days between first and last purchase in month
#'   If include_quarters = TRUE, additional columns:
#'   - quarter: Quarter identifier (e.g., "2024-Q1")
#'   - total_spending_quarterly: Total spending in the quarter
#'   - purchase_frequency_quarterly: Number of purchases in the quarter
#'
#' @examples
#' \dontrun{
#' # Basic usage with default column names
#' library(dplyr)
#' library(lubridate)
#' 
#' # Create sample data
#' purchase_data <- data.frame(
#'   customer_id = rep(c("CUST001", "CUST002", "CUST003"), each = 10),
#'   purchase_date = seq(as.Date("2024-01-01"), 
#'                      as.Date("2024-10-31"), 
#'                      length.out = 30),
#'   purchase_amount = round(runif(30, 50, 500), 2),
#'   order_id = paste0("ORD", sprintf("%05d", 1:30))
#' )
#' 
#' # Aggregate by month
#' monthly_summary <- aggregate_customer_purchases(purchase_data)
#' 
#' # Aggregate with quarterly data
#' quarterly_summary <- aggregate_customer_purchases(
#'   purchase_data, 
#'   include_quarters = TRUE
#' )
#' 
#' # Custom column names
#' custom_data <- data.frame(
#'   cust_code = rep(1:3, each = 10),
#'   order_date = seq(as.Date("2024-01-01"), 
#'                   as.Date("2024-10-31"), 
#'                   length.out = 30),
#'   amount = round(runif(30, 50, 500), 2)
#' )
#' 
#' custom_summary <- aggregate_customer_purchases(
#'   custom_data,
#'   date_column = "order_date",
#'   amount_column = "amount",
#'   customer_column = "cust_code"
#' )
#' }
#'
#' @author MAMBA System
#' @date 2025-08-23
#' @version 1.0.0
#'
#' @import dplyr
#' @import lubridate
#' @importFrom stats aggregate
#' @export
#'
aggregate_customer_purchases <- function(
  purchase_data,
  date_column = "purchase_date",
  amount_column = "purchase_amount", 
  customer_column = "customer_id",
  include_quarters = FALSE,
  fiscal_year_start = 1
) {
  
  # =========================
  # Input Validation
  # =========================
  
  # Validate purchase_data
  if (!is.data.frame(purchase_data)) {
    stop("purchase_data must be a data frame")
  }
  
  if (nrow(purchase_data) == 0) {
    warning("purchase_data has no rows, returning empty data frame")
    return(create_empty_result_frame(include_quarters))
  }
  
  # Validate required columns exist
  required_columns <- c(customer_column, date_column, amount_column)
  missing_columns <- setdiff(required_columns, names(purchase_data))
  
  if (length(missing_columns) > 0) {
    stop(paste("Missing required columns:", paste(missing_columns, collapse = ", ")))
  }
  
  # Validate data types
  if (!is.numeric(purchase_data[[amount_column]])) {
    stop(paste(amount_column, "must be numeric"))
  }
  
  # =========================
  # Data Preparation
  # =========================
  
  # Create working copy with standardized column names
  working_data <- purchase_data %>%
    select(
      customer_id = all_of(customer_column),
      purchase_date = all_of(date_column),
      purchase_amount = all_of(amount_column)
    )
  
  # Convert date column to Date type if needed
  if (!inherits(working_data$purchase_date, "Date")) {
    working_data$purchase_date <- as.Date(working_data$purchase_date)
  }
  
  # Remove rows with NA values in critical columns
  initial_rows <- nrow(working_data)
  working_data <- working_data %>%
    filter(
      !is.na(customer_id),
      !is.na(purchase_date),
      !is.na(purchase_amount)
    )
  
  rows_removed <- initial_rows - nrow(working_data)
  if (rows_removed > 0) {
    message(sprintf("Removed %d rows with NA values", rows_removed))
  }
  
  # =========================
  # Monthly Aggregation
  # =========================
  
  # Add year-month column for grouping
  working_data <- working_data %>%
    mutate(
      year = year(purchase_date),
      month = month(purchase_date),
      year_month = sprintf("%04d-%02d", year, month)
    )
  
  # Calculate monthly aggregations
  monthly_summary <- working_data %>%
    group_by(customer_id, year_month) %>%
    summarise(
      # Total spending in the month
      total_spending_monthly = sum(purchase_amount, na.rm = TRUE),
      
      # Number of purchases (frequency)
      purchase_frequency_monthly = n(),
      
      # Average purchase amount
      average_purchase_amount_monthly = mean(purchase_amount, na.rm = TRUE),
      
      # First and last purchase dates in month
      first_purchase_date_monthly = min(purchase_date, na.rm = TRUE),
      last_purchase_date_monthly = max(purchase_date, na.rm = TRUE),
      
      .groups = "drop"
    ) %>%
    mutate(
      # Days between first and last purchase
      days_between_first_last_monthly = as.numeric(
        difftime(last_purchase_date_monthly, 
                first_purchase_date_monthly, 
                units = "days")
      )
    )
  
  # =========================
  # Quarterly Aggregation (Optional)
  # =========================
  
  if (include_quarters) {
    quarterly_data <- working_data %>%
      mutate(
        # Calculate fiscal quarter based on fiscal_year_start
        fiscal_month = ((month - fiscal_year_start) %% 12) + 1,
        fiscal_quarter = ceiling(fiscal_month / 3),
        fiscal_year = ifelse(month >= fiscal_year_start, year, year - 1),
        quarter = sprintf("%04d-Q%d", fiscal_year, fiscal_quarter)
      )
    
    quarterly_summary <- quarterly_data %>%
      group_by(customer_id, quarter) %>%
      summarise(
        total_spending_quarterly = sum(purchase_amount, na.rm = TRUE),
        purchase_frequency_quarterly = n(),
        .groups = "drop"
      )
    
    # Merge quarterly data with monthly summary
    # First, extract quarter from year_month for joining
    monthly_summary <- monthly_summary %>%
      mutate(
        year = as.integer(substr(year_month, 1, 4)),
        month = as.integer(substr(year_month, 6, 7)),
        fiscal_month = ((month - fiscal_year_start) %% 12) + 1,
        fiscal_quarter = ceiling(fiscal_month / 3),
        fiscal_year = ifelse(month >= fiscal_year_start, year, year - 1),
        quarter = sprintf("%04d-Q%d", fiscal_year, fiscal_quarter)
      ) %>%
      left_join(quarterly_summary, by = c("customer_id", "quarter")) %>%
      select(-year, -month, -fiscal_month, -fiscal_quarter, -fiscal_year)
  }
  
  # =========================
  # Final Formatting
  # =========================
  
  # Round numeric columns for cleaner output
  monthly_summary <- monthly_summary %>%
    mutate(
      total_spending_monthly = round(total_spending_monthly, 2),
      average_purchase_amount_monthly = round(average_purchase_amount_monthly, 2)
    )
  
  if (include_quarters) {
    monthly_summary <- monthly_summary %>%
      mutate(
        total_spending_quarterly = round(total_spending_quarterly, 2)
      )
  }
  
  # Sort by customer and date
  monthly_summary <- monthly_summary %>%
    arrange(customer_id, year_month)
  
  return(monthly_summary)
}

#' Create Empty Result Frame
#'
#' Helper function to create an empty data frame with the correct structure
#' when input data is empty.
#'
#' @param include_quarters Logical indicating whether to include quarterly columns
#' @return Empty data frame with appropriate column structure
#' @keywords internal
#'
create_empty_result_frame <- function(include_quarters = FALSE) {
  
  base_frame <- data.frame(
    customer_id = character(),
    year_month = character(),
    total_spending_monthly = numeric(),
    purchase_frequency_monthly = integer(),
    average_purchase_amount_monthly = numeric(),
    first_purchase_date_monthly = as.Date(character()),
    last_purchase_date_monthly = as.Date(character()),
    days_between_first_last_monthly = numeric(),
    stringsAsFactors = FALSE
  )
  
  if (include_quarters) {
    base_frame$quarter <- character()
    base_frame$total_spending_quarterly <- numeric()
    base_frame$purchase_frequency_quarterly <- integer()
  }
  
  return(base_frame)
}

#' Validate Fiscal Year Start
#'
#' Helper function to validate the fiscal_year_start parameter.
#'
#' @param fiscal_year_start Integer indicating the starting month
#' @return Validated fiscal year start value
#' @keywords internal
#'
validate_fiscal_year_start <- function(fiscal_year_start) {
  
  if (!is.numeric(fiscal_year_start) || 
      fiscal_year_start < 1 || 
      fiscal_year_start > 12) {
    warning("Invalid fiscal_year_start, using default value of 1 (January)")
    return(1)
  }
  
  return(as.integer(fiscal_year_start))
}