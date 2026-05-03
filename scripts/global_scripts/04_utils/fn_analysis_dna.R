library(data.table)
library(dplyr)
library(caret)
library(forcats)
library(lubridate)

# Define copy explicitly to avoid method conflicts
dt_copy <- data.table::copy

#' Customer DNA Analysis Function
#'
#' @description
#' Analyzes customer behavior patterns and computes various customer metrics
#' including RFM (Recency, Frequency, Monetary), Customer Activity Index (CAI),
#' Past Customer Value (PCV), Customer Lifetime Value (CLV), and more.
#'
#' IMPORTANT: This function requires specific fields to be present in the input data.
#' Required fields include:
#' - In df_sales_by_customer_by_date: customer_id, and one of: payment_time, min_time_by_date, or min_time
#' - In df_sales_by_customer: 
#'   - ipt: Required for inter-purchase time
#'   - total_spent (or alternatives: sum_sales_by_customer, sum_spent_by_date, total_amount)
#'   - times (or alternatives: sum_transactions_by_customer, count_transactions_by_date)
#'   - ni: Required for purchase frequency
#' 
#' Note: Field names must be lowercase. Uppercase field names are no longer accepted.
#' 
#' IMPORTANT: The df_sales_by_customer_id data must contain exactly one row per customer_id.
#' Both input (df_sales_by_customer_id) and output (data_by_customer) follow the principle that
#' customer-level data must have exactly one row per customer.
#' 
#' If any required field is missing, the function will stop with an error.
#'
#' @param df_sales_by_customer Aggregated data by customer_id with pre-calculated ipt values.
#'        This parameter is REQUIRED and must contain the ipt field (lowercase only).
#'        This must include:
#'        - ipt: Inter-purchase time value
#'        - total_spent (or alternatives: sum_sales_by_customer, sum_spent_by_date, total_amount)
#'        - times (or alternatives: sum_transactions_by_customer, count_transactions_by_date)
#'        - ni: Number of purchases/transactions
#'        All field names must use lowercase as per the naming convention.
#'        This is the customer-level aggregated data (one row per customer).
#' @param df_sales_by_customer_by_date Transaction data with customer_id and time information (payment_time, min_time_by_date, or min_time), etc. aggregated by date.
#'        This is the detailed transaction data with multiple rows per customer (one row per customer per date).
#' @param skip_within_subject Logical; if TRUE, skips within-subject calculations for faster processing
#' @param verbose Logical; if TRUE, outputs progress messages
#' @param global_params List; Optional list of global parameters (delta, ni_threshold, etc.).
#'        If not provided, will attempt to find the parameters in the global environment or use defaults.
#'
#' @return List containing:
#'   - data_by_customer: A tibble with customer DNA metrics
#'   - nrec_accu: Accuracy metrics for the churn prediction model
#'
#' @examples
#' # Basic usage with the new parameter structure
#' results <- analysis_dna(df_sales_by_customer, df_sales_by_customer_by_date)
#'
#' # Skip within-subject analysis for better performance
#' results <- analysis_dna(df_sales_by_customer, df_sales_by_customer_by_date, skip_within_subject = TRUE)
#'
#' # Loading data from a database connection
#' df_ebay_sales_by_customer <- tbl(processed_data, "df_ebay_sales_by_customer") %>% collect()
#' df_ebay_sales_by_customer_by_date <- tbl(processed_data, "df_ebay_sales_by_customer_by_date") %>% collect()
#' # Note: The function will automatically detect time fields like min_time_by_date or min_time if payment_time is not available
#' dna_results <- analysis_dna(df_ebay_sales_by_customer, df_ebay_sales_by_customer_by_date)
#' 
#' # The function strictly requires lowercase field names (e.g., "ipt" not "IPT")
#'
analysis_dna <- function(df_sales_by_customer, df_sales_by_customer_by_date, skip_within_subject = FALSE, verbose = TRUE, global_params = NULL) {
  # Map the incoming arguments to match the original function design
  df_sales <- df_sales_by_customer_by_date  # This contains the detailed transaction data
  df_sales_by_customer_id <- df_sales_by_customer  # This contains the customer-level aggregated data
  # Start timing
  total_start_time <- Sys.time()
  
  if (verbose) message("Starting customer DNA analysis...")
  
  # MP045: Automatic Data Availability Detection - report on input data structure
  if (verbose) {
    message("Input data dimensions: sales_by_customer (", nrow(df_sales_by_customer), " rows, ", ncol(df_sales_by_customer), " cols), sales_by_customer_by_date (", nrow(df_sales_by_customer_by_date), " rows, ", ncol(df_sales_by_customer_by_date), " cols)")
  }
  
  # Check for required utility functions
  if (!exists("left_join_remove_duplicate2")) {
    if (file.exists(file.path("update_scripts", "global_scripts", "04_utils", "fn_left_join_remove_duplicate2.R"))) {
      source(file.path("update_scripts", "global_scripts", "04_utils", "fn_left_join_remove_duplicate2.R"))
    } else {
      stop("Required utility function 'left_join_remove_duplicate2' not found. Please source fn_left_join_remove_duplicate2.R first.")
    }
  }
  
  if (!exists("fct_na_value_to_level")) {
    if (file.exists(file.path("update_scripts", "global_scripts", "04_utils", "fn_fct_na_value_to_level.R"))) {
      source(file.path("update_scripts", "global_scripts", "04_utils", "fn_fct_na_value_to_level.R"))
    } else {
      stop("Required utility function 'fct_na_value_to_level' not found. Please source fn_fct_na_value_to_level.R first.")
    }
  }
  
  # Set up parameters
  # List of required parameters
  required_params <- c("delta", "ni_threshold", "cai_breaks", "text_cai_label", 
                      "f_breaks", "text_f_label", "r_breaks", "text_r_label", 
                      "m_breaks", "text_m_label", "nes_breaks", "text_nes_label")
  
  # If global_params is provided, use those
  if (!is.null(global_params)) {
    # Check if all required parameters are in the provided list
    missing_params <- required_params[!required_params %in% names(global_params)]
    if (length(missing_params) > 0) {
      if (verbose) message("Missing parameters in provided global_params: ", paste(missing_params, collapse = ", "))
    } else {
      # Extract all parameters from the provided list
      for (param in required_params) {
        assign(param, global_params[[param]])
      }
    }
  } else {
    # Check global environment for parameters
    missing_params <- required_params[!sapply(required_params, exists)]
    
    # If any parameters are missing, either load them or use defaults
    if (length(missing_params) > 0) {
      # Try to get parameters from global_parameters.R but don't source directly
      global_params_path <- file.path("update_scripts", "global_scripts", "03_config", "global_parameters.R")
      
      if (file.exists(global_params_path)) {
        if (verbose) message("Loading parameters from global_parameters.R...")
        
        # Use a new environment to avoid polluting global environment
        param_env <- new.env()
        source(global_params_path, local = param_env)
        
        # Check if the parameters exist in the loaded environment
        for (param in required_params) {
          if (exists(param, envir = param_env)) {
            assign(param, get(param, envir = param_env))
          }
        }
        
        # Check which parameters are still missing
        missing_params <- required_params[!sapply(required_params, exists)]
      }
      
      # If still missing parameters, use defaults
      if (length(missing_params) > 0) {
        if (verbose) message("Using default parameters for: ", paste(missing_params, collapse = ", "))
        
        # Default parameter values
        default_params <- list(
          ni_threshold = 4,
          delta = 0.0001,
          cai_breaks = c(0, 0.1, 0.9, 1),
          text_cai_label = c("Gradually Inactive", "Stable", "Increasingly Active"),
          f_breaks = c(-0.0001, 1.1, 2.1, Inf),
          text_f_label = c("Low Frequency", "Medium Frequency", "High Frequency"),
          r_breaks = c(-0.0001, 0.1, 0.9, 1.0001),
          text_r_label = c("Long Inactive", "Medium Inactive", "Recent Buyer"),
          m_breaks = c(-0.0001, 0.1, 0.9, 1.0001),
          text_m_label = c("Low Value", "Medium Value", "High Value"),
          nes_breaks = c(0, 1, 2, 2.5, Inf),
          text_nes_label = c("E0", "S1", "S2", "S3")
        )
        
        # Assign default values for missing parameters
        for (param in missing_params) {
          if (param %in% names(default_params)) {
            assign(param, default_params[[param]])
          }
        }
      }
    }
  }
  
  # Input data validation
  if (nrow(df_sales) == 0) {
    stop("Empty input data. Cannot proceed with analysis.")
  }
  
  # Validate that df_sales_by_customer_id is provided and contains ipt
  if (is.null(df_sales_by_customer_id)) {
    stop("ERROR: df_sales_by_customer_id is required and cannot be NULL.")
  }
  
  # Check for lowercase "ipt"
  if (!("ipt" %in% names(df_sales_by_customer_id))) {
    stop("ERROR: ipt field is missing in df_sales_by_customer_id. This field is required for DNA analysis.")
  }
  
  # If uppercase "IPT" exists, remove it to avoid confusion
  if ("IPT" %in% names(df_sales_by_customer_id)) {
    if (verbose) message("Removing uppercase 'IPT' field to ensure consistency with lowercase naming convention")
    df_sales_by_customer_id$IPT <- NULL
  }
  
  # Check required fields in df_sales for customer_id and a time field (payment_time or min_time_by_date)
  # First check for customer_id
  if (!("customer_id" %in% names(df_sales))) {
    stop("ERROR: Required field 'customer_id' missing in df_sales")
  }
  
  # Then check for time fields with possible alternatives
  time_field_options <- c("payment_time", "min_time_by_date", "min_time")
  available_time_fields <- time_field_options[time_field_options %in% names(df_sales)]
  
  if (length(available_time_fields) == 0) {
    stop("ERROR: No suitable time field found in df_sales. Required one of: ", paste(time_field_options, collapse=", "))
  }
  
  # Use the first available time field and rename it to payment_time for internal use
  time_field_to_use <- available_time_fields[1]
  if (verbose) message("Using '", time_field_to_use, "' as the payment time field")
  
  if (time_field_to_use != "payment_time") {
    df_sales$payment_time <- df_sales[[time_field_to_use]]
  }
  
  # Check for required fields in df_sales_by_customer_id - now case insensitive
  # Define fields with potential variations, using lowercase as the standard
  field_variations <- list(
    "ipt" = c("ipt"),
    "total_amount" = c("total_amount", "total_spent", "sum_sales_by_customer", "sum_spent_by_date"),
    "times" = c("times", "sum_transactions_by_customer", "count_transactions_by_date"),
    "ni" = c("ni")
  )
  
  missing_fields <- c()
  
  # For each required field, check if any of its variations exist
  for (field in names(field_variations)) {
    variations <- field_variations[[field]]
    # Check if at least one variation exists
    if (!any(variations %in% names(df_sales_by_customer_id))) {
      missing_fields <- c(missing_fields, field)
    }
    # If the canonical form doesn't exist but a variation does, create the canonical form
    if (!(field %in% names(df_sales_by_customer_id))) {
      # Find the first variation that exists
      existing_variations <- variations[variations %in% names(df_sales_by_customer_id)]
      if (length(existing_variations) > 0) {
        if (verbose) message("Converting '", existing_variations[1], "' field to '", field, "' for compatibility")
        df_sales_by_customer_id[[field]] <- df_sales_by_customer_id[[existing_variations[1]]]
      }
    }
  }
  
  if (length(missing_fields) > 0) {
    stop("ERROR: Required fields missing in df_sales_by_customer_id: ", paste(missing_fields, collapse=", "), 
         ". These fields must be pre-calculated in the customer aggregated data.")
  }
  
  # Check that customer_id is unique in df_sales_by_customer_id
  if (anyDuplicated(df_sales_by_customer_id$customer_id)) {
    stop("ERROR: customer_id must be unique in df_sales_by_customer_id. Each customer should appear exactly once.")
  }
  
  # We're now going to work directly with df_sales_by_customer_id for the required fields
  if (verbose) message("Using pre-calculated fields from df_sales_by_customer_id")
  
  # Create a list to store the columns we want to extract
  columns_to_extract <- c("customer_id", "ipt", "ni")
  
  # Find the appropriate column for total_spent
  total_spent_options <- c("total_spent", "sum_sales_by_customer", "sum_spent_by_date", "total_amount")
  for (col in total_spent_options) {
    if (col %in% names(df_sales_by_customer_id)) {
      columns_to_extract <- c(columns_to_extract, col)
      if (col != "total_spent") {
        # If we found an alternative column, create an alias
        df_sales_by_customer_id$total_spent <- df_sales_by_customer_id[[col]]
        if (verbose) message("Using '", col, "' as the total spent field")
      }
      break
    }
  }
  
  # Find the appropriate column for times
  times_options <- c("times", "sum_transactions_by_customer", "count_transactions_by_date")
  times_found <- FALSE
  for (col in times_options) {
    if (col %in% names(df_sales_by_customer_id)) {
      columns_to_extract <- c(columns_to_extract, col)
      if (col != "times") {
        # If we found an alternative column, create an alias
        df_sales_by_customer_id$times <- df_sales_by_customer_id[[col]]
        if (verbose) message("Using '", col, "' as the times field")
      }
      times_found <- TRUE
      break
    }
  }
  
  # If no times field found, create one from ni
  if (!times_found && "ni" %in% names(df_sales_by_customer_id)) {
    df_sales_by_customer_id$times <- df_sales_by_customer_id$ni
    if (verbose) message("Using 'ni' as the times field")
    times_found <- TRUE
  }
  
  # Final check - if still no times field, throw error with helpful message
  if (!times_found) {
    available_cols <- paste(names(df_sales_by_customer_id), collapse = ", ")
    stop("ERROR: No suitable 'times' field found. Available columns: ", available_cols, 
         ". Expected one of: ", paste(times_options, collapse = ", "), " or 'ni'")
  }
  
  # Extract the fields we need into data_by_customer
  # First ensure all required columns exist
  required_cols <- c("customer_id", "ipt", "total_spent", "times")
  missing_cols <- required_cols[!required_cols %in% names(df_sales_by_customer_id)]
  if (length(missing_cols) > 0) {
    stop("ERROR: Required columns missing: ", paste(missing_cols, collapse = ", "))
  }
  
  data_by_customer <- as.data.table(df_sales_by_customer_id[, required_cols])
  # Ensure we maintain the one-row-per-customer principle by setting the key
  setkey(data_by_customer, customer_id)
  
  # Double-check there are no duplicates (should be redundant after the earlier check, but safety first)
  if (anyDuplicated(data_by_customer$customer_id)) {
    # Instead of stopping, we'll take the unique values to ensure one-row-per-customer
    if (verbose) message("Warning: Duplicate customer_id values found in the processed data. Taking unique values.")
    data_by_customer <- unique(data_by_customer, by = "customer_id")
  }
  
  # Check if we need to convert customer_id format
  # If customer_id in both datasets are consistent and already processed by modules, skip conversion
  
  # Check if customer_id types are consistent between datasets
  sales_id_type <- class(df_sales$customer_id)[1]
  customer_id_type <- class(df_sales_by_customer_id$customer_id)[1]
  
  if (verbose) message("Customer ID types - Sales data: ", sales_id_type, ", Customer data: ", customer_id_type)
  
  # Only attempt conversion if there's a type mismatch or if both are character
  if (!is.numeric(df_sales$customer_id) && (sales_id_type != customer_id_type || sales_id_type == "character")) {
    if (verbose) message("Converting customer_id to numeric format...")
    
    # First try to convert - but handle cases where it's already integer-like
    tryCatch({
      # Check if all values are integer-like (even if stored as character)
      test_conversion <- suppressWarnings(as.numeric(df_sales$customer_id))
      na_count <- sum(is.na(test_conversion))
      
      if (na_count == 0) {
        # All values converted successfully
        df_sales <- df_sales %>%
          mutate(customer_id = as.numeric(customer_id))
        if (verbose) message("Successfully converted all customer_id values to numeric")
      } else if (na_count < nrow(df_sales) * 0.1) {
        # Less than 10% failed conversion - likely some bad data
        df_sales <- df_sales %>%
          mutate(customer_id = as.numeric(customer_id))
        if (verbose) message("Converted customer_id to numeric with ", na_count, " NA values (", 
                            round(na_count/nrow(df_sales)*100, 1), "%)")
      } else {
        # Too many failures - keep original format but warn
        if (verbose) message("Customer_id appears to contain non-numeric values. Keeping original format.")
        if (verbose) message("This is expected when customer_id has already been processed by upload modules.")
      }
    }, error = function(e) {
      if (verbose) message("Customer_id conversion skipped: ", e$message, ". Using original format.")
    })
  } else {
    if (verbose) message("Customer_id types are consistent or already numeric. Skipping conversion.")
  }

  # CRITICAL: Ensure df_sales_by_customer_id has the same customer_id type as df_sales
  # This prevents data.table merge errors like "Incompatible join types"
  df_sales_id_class <- class(df_sales$customer_id)[1]
  df_customer_id_class <- class(df_sales_by_customer_id$customer_id)[1]
  if (df_sales_id_class != df_customer_id_class) {
    if (verbose) message("Synchronizing customer_id types between datasets...")
    if (df_sales_id_class == "numeric" || df_sales_id_class == "integer") {
      df_sales_by_customer_id$customer_id <- suppressWarnings(as.numeric(df_sales_by_customer_id$customer_id))
      # Also update data_by_customer which was created before this sync
      data_by_customer$customer_id <- suppressWarnings(as.numeric(data_by_customer$customer_id))
    } else {
      df_sales_by_customer_id$customer_id <- as.character(df_sales_by_customer_id$customer_id)
      df_sales$customer_id <- as.character(df_sales$customer_id)
      data_by_customer$customer_id <- as.character(data_by_customer$customer_id)
    }
    if (verbose) message("Customer_id types synchronized to: ", class(df_sales$customer_id)[1])
  }

  # Convert df_sales to data.table for payment_time-related operations
  dt <- as.data.table(df_sales)
  setkey(dt, customer_id)
  
  # Ensure we have a total_spent field in dt
  total_spent_options <- c("total_spent", "sum_spent_by_date", "sum_sales_by_customer", "total_amount")
  if (!("total_spent" %in% names(dt)) && any(total_spent_options %in% names(dt))) {
    for (col in total_spent_options) {
      if (col %in% names(dt)) {
        dt[, total_spent := get(col)]
        if (verbose) message("Using '", col, "' as the transaction total spent field")
        break
      }
    }
  }
  
  if (verbose) message("Processing customer data...")
  
  # D01 Field Detection: Ensure we have essential fields for DNA analysis
  if (!("times" %in% names(dt))) {
    if ("count_transactions_by_date" %in% names(dt)) {
      dt[, times := count_transactions_by_date]
      if (verbose) message("Using 'count_transactions_by_date' as 'times' field")
    } else {
      # Create times field from row sequence per customer based on payment_time
      dt <- dt[order(customer_id, payment_time)]
      dt[, times := seq_len(.N), by = customer_id]
      if (verbose) message("Created 'times' field from transaction sequence per customer")
    }
  }
  
  # Add geographic columns if they exist (MP045: Automatic Data Availability Detection)
  geo_cols <- c("zipcode", "state", "lat", "lng")
  missing_geo_cols <- c()
  
  for (col in geo_cols) {
    if (col %in% names(dt)) {
      # For each column that exists, add it to data_by_customer
      data_by_customer[, (col) := dt[data_by_customer, first(get(col)), by = customer_id]$V1]
    } else {
      # For missing columns, add NA values
      missing_geo_cols <- c(missing_geo_cols, col)
      data_by_customer[, (col) := NA]
    }
  }
  
  # Report missing geographic fields once (following D01 principles for optional fields)
  if (length(missing_geo_cols) > 0 && verbose) {
    message("Geographic fields not found in data (using NA values): ", paste(missing_geo_cols, collapse = ", "))
    message("This is expected when processing data without geographic information.")
  }
  
  # Current timestamp for recency calculations (D01 Data Standardization)
  # Handle the case where all payment_times are NA or -Inf is returned
  if (verbose) message("Determining reference time for recency calculations...")
  
  # Ensure payment_time column is in proper datetime format before processing
  if (!inherits(dt$payment_time, c("POSIXct", "POSIXt", "Date"))) {
    dt[, payment_time := as.POSIXct(payment_time)]
    if (verbose) message("Converted payment_time column to POSIXct format")
  }
  
  # Calculate time_now safely
  time_now <- tryCatch({
    max_time <- max(dt$payment_time, na.rm = TRUE)
    if (is.infinite(max_time) || is.na(max_time)) {
      if (verbose) message("Warning: Cannot determine maximum payment_time from data. Using current time.")
      Sys.time()
    } else {
      max_time
    }
  }, error = function(e) {
    if (verbose) message("Error calculating max payment_time: ", e$message, ". Using current time.")
    Sys.time()
  })
  
  if (verbose) message("Reference time established: ", as.character(time_now))
  
  #--------------------------------------------
  # 2. RFM Calculation
  if (verbose) message("Calculating RFM metrics...")
  start_RFM <- Sys.time()
  
  # Calculate m_value and other metrics using the pre-calculated ipt (lowercase)
  # We've already ensured that ipt exists in df_sales_by_customer_id and created total_spent if needed
  
  # D01 Field Detection: Ensure we have the 'ni' field (number of transactions per customer)
  if (!("ni" %in% names(df_sales_by_customer_id))) {
    if ("times" %in% names(df_sales_by_customer_id)) {
      df_sales_by_customer_id$ni <- df_sales_by_customer_id$times
      if (verbose) message("Using 'times' field as 'ni' (number of transactions per customer)")
    } else {
      # Create ni field by counting transactions per customer from the detailed data
      ni_counts <- dt[, .(ni = .N), by = customer_id]
      df_sales_by_customer_id <- merge(as.data.table(df_sales_by_customer_id), ni_counts, by = "customer_id", all.x = TRUE)
      if (verbose) message("Created 'ni' field from transaction count per customer")
    }
  }
  
  # Extract the data we need
  # Include min/max_time_by_date for correct ipt_mean calculation if available
  base_cols <- c("customer_id", "ipt", "total_spent", "ni")
  time_cols <- intersect(c("min_time_by_date", "max_time_by_date"), names(df_sales_by_customer_id))
  select_cols <- c(base_cols, time_cols)
  # Ensure df_sales_by_customer_id is a data.table before using data.table syntax
  if (!inherits(df_sales_by_customer_id, "data.table")) {
    df_sales_by_customer_id <- as.data.table(df_sales_by_customer_id)
  }
  ipt_table <- df_sales_by_customer_id[, select_cols, with = FALSE]

  # Compute additional metrics
  # ipt_mean: average inter-purchase time = total_span / (ni - 1)
  # Note: ipt from upstream is just the first inter-purchase interval, not total span
  has_time_cols <- all(c("min_time_by_date", "max_time_by_date") %in% names(ipt_table))

  if (has_time_cols) {
    # Correct calculation: time span from first to last purchase divided by (purchases - 1)
    ipt_table[, `:=`(
      ipt_mean = ifelse(ni <= 1, NA_real_,
                        as.numeric(difftime(max_time_by_date, min_time_by_date, units = "days")) / (ni - 1)),
      m_value = total_spent,
      sigma_hnorm_mle = sqrt(ipt^2),
      sigma_hnorm_bcmle = sqrt(ipt^2) + (sqrt(ipt^2)/(4*(1)))
    )]
  } else {
    # Fallback: use ipt directly (legacy behavior, less accurate)
    if (verbose) warning("min/max_time_by_date not found - using legacy ipt_mean calculation")
    ipt_table[, `:=`(
      ipt_mean = ipt,
      m_value = total_spent,
      sigma_hnorm_mle = sqrt(ipt^2),
      sigma_hnorm_bcmle = sqrt(ipt^2) + (sqrt(ipt^2)/(4*(1)))
    )]
  }
  
  ipt_table[, m_ecdf := cume_dist(m_value)]
  ipt_table[, m_label := cut(m_ecdf, m_breaks, labels = text_m_label, right = TRUE, ordered_result = TRUE)]
  
  # Merge ipt_table results into data_by_customer
  ipt_table <- ipt_table[, .(customer_id, ipt_mean, m_value, ni, sigma_hnorm_mle, sigma_hnorm_bcmle, m_ecdf, m_label)]
  # Ensure ipt_table has unique customer_id to maintain the one-row-per-customer principle
  ipt_table <- unique(ipt_table, by = "customer_id")
  data_by_customer <- merge(data_by_customer, ipt_table, by = "customer_id", all.x = TRUE)
  
  # Double-check that we maintain one row per customer after merge
  data_by_customer <- unique(data_by_customer, by = "customer_id")
  
  # Clean up any duplicated column names that might have been created during merges
  # This happens when merging tables with the same column names (like ni, etc.)
  clean_colnames <- function(dt) {
    # Find columns ending with .x and replace them
    pattern <- "\\.x$"
    x_cols <- grep(pattern, names(dt), value = TRUE)
    
    # For each .x column, if there's a corresponding .y column, remove both and keep just the base name
    for (col in x_cols) {
      base_col <- sub(pattern, "", col)
      y_col <- paste0(base_col, ".y")
      
      # If both .x and .y exist, use the .x value and drop both columns
      if (y_col %in% names(dt)) {
        if (verbose) message("Fixing duplicated column: ", base_col)
        dt[[base_col]] <- dt[[col]]  # Use the .x value
        dt[[col]] <- NULL  # Remove .x column
        dt[[y_col]] <- NULL  # Remove .y column
      } else {
        # Just rename .x to the base name if no .y exists
        setnames(dt, col, base_col)
      }
    }
    
    # Check for any remaining .y columns and rename them
    y_cols <- grep("\\.y$", names(dt), value = TRUE)
    for (col in y_cols) {
      base_col <- sub("\\.y$", "", col)
      if (!(base_col %in% names(dt))) {
        setnames(dt, col, base_col)
      } else {
        # If base already exists, just drop the .y column
        dt[[col]] <- NULL
      }
    }
    
    return(dt)
  }
  
  # Clean up any duplicate column names
  data_by_customer <- clean_colnames(data_by_customer)
  
  if (verbose) message(paste("RFM analysis completed in", round(difftime(Sys.time(), start_RFM, units = "secs"), 2), "seconds"))
  if (verbose) message(paste("Current data has", nrow(data_by_customer), "unique customers"))
  
  #--------------------------------------------
  # 3. NES Calculation
  if (verbose) message("Calculating NES metrics...")
  start_NES <- Sys.time()
  
  # For transaction-level analysis, we don't have the one-row-per-customer constraint
  # But we need unique customer-level data in ipt_table
  # Following D01 principles: ensure clean merge by using select before merge to avoid column conflicts
  ipt_table_clean <- unique(ipt_table, by = "customer_id")[, .(customer_id, ipt_mean, m_value, ni, sigma_hnorm_mle, sigma_hnorm_bcmle)]
  nes_dt <- merge(dt, ipt_table_clean, by = "customer_id", all.x = TRUE, suffix = c("", "_ipt"))
  
  #### fix point - clean column names following D01 data standardization ####
  nes_dt <- clean_colnames(nes_dt)
  if (verbose) message("NES data merge completed, processing ratio calculations...")
  
  # D01 Field Detection: Ensure we have a 'times' field for NES calculation
  if (!("times" %in% names(nes_dt))) {
    if ("count_transactions_by_date" %in% names(nes_dt)) {
      nes_dt[, times := count_transactions_by_date]
      if (verbose) message("Using 'count_transactions_by_date' as 'times' field for NES calculation")
    } else if ("sum_transactions_by_customer" %in% names(nes_dt)) {
      nes_dt[, times := sum_transactions_by_customer]
      if (verbose) message("Using 'sum_transactions_by_customer' as 'times' field for NES calculation")
    } else {
      # Create times field from row sequence per customer if no other option
      nes_dt[, times := .N, by = customer_id]
      if (verbose) message("Creating 'times' field from customer transaction count for NES calculation")
    }
  }
  
  # Get record with maximum times (most recent) for each customer
  if (verbose) message("Computing most recent transactions per customer...")
  max_idx <- nes_dt[, .I[which.max(times)], by = customer_id]$V1
  nes_dt <- nes_dt[max_idx]
  if (verbose) message("Processing ", nrow(nes_dt), " customer records for NES calculation...")
  
  nes_dt[, ipt_mean := as.difftime(ipt_mean, units = "days")]
  nes_dt[, difftime := difftime(time_now, payment_time, units = "days")]
  nes_dt[, nes_ratio := as.numeric(difftime) / as.numeric(ipt_mean)]
  if (verbose) message("NES ratio calculations completed...")
  
  # Use a fixed NES median value
  nes_median <- 1.7  # Fixed value based on domain knowledge
  if (verbose) message("Using fixed nes_median value: ", nes_median)
  
  nes_dt[, nes_status := fct_na_value_to_level(
    cut(nes_ratio, breaks = nes_breaks * nes_median, labels = text_nes_label, right = FALSE, ordered_result = TRUE),
    level = "N")]
  nes_dt[, nes_status := ordered(nes_status, levels = c("N", text_nes_label))]
  
  # Merge NES results into data_by_customer - ensure customer uniqueness
  nes_dt <- unique(nes_dt, by = "customer_id")
  data_by_customer <- left_join_remove_duplicate2(data_by_customer, nes_dt)
  # Ensure we maintain one row per customer
  data_by_customer <- unique(data_by_customer, by = "customer_id")
  
  if (verbose) message(paste("NES analysis completed in", round(difftime(Sys.time(), start_NES, units = "secs"), 2), "seconds"))
  
  #--------------------------------------------
  # 4. R and F Labeling
  if (verbose) message("Calculating recency and frequency labels...")
  start_RF <- Sys.time()
  
  # Ensure we have a times field for recency/frequency calculations
  if (!("times" %in% names(dt))) {
    if ("count_transactions_by_date" %in% names(dt)) {
      dt[, times := count_transactions_by_date]
      if (verbose) message("Using 'count_transactions_by_date' for RF calculations")
    } else if ("sum_transactions_by_customer" %in% names(dt)) {
      dt[, times := sum_transactions_by_customer]
      if (verbose) message("Using 'sum_transactions_by_customer' for RF calculations")
    }
  }
  
  r_f_dt <- dt[dt[, .I[which.max(as.numeric(times))], by = customer_id]$V1, .(customer_id, times, payment_time)]
  # Use data.table::copy directly to avoid inheritance method issues
  r_f_dt2 <- dt_copy(r_f_dt)
  
  # D01 Data Type Standardization: Ensure proper datetime handling for time calculations
  if (verbose) message("Validating datetime fields for recency calculations...")
  
  # Ensure payment_time is in proper datetime format
  if (!inherits(r_f_dt2$payment_time, c("POSIXct", "POSIXt", "Date"))) {
    r_f_dt2[, payment_time := as.POSIXct(payment_time)]
    if (verbose) message("Converted payment_time to POSIXct format")
  }
  
  # Ensure time_now is in proper datetime format
  if (!inherits(time_now, c("POSIXct", "POSIXt", "Date"))) {
    time_now <- as.POSIXct(time_now)
    if (verbose) message("Converted time_now to POSIXct format")
  }
  
  # Calculate r_value using robust time difference calculation
  r_f_dt2[, r_value := as.numeric(difftime(time_now, payment_time, units = "days"))]
  r_f_dt2[, f_value := times]
  r_f_dt2[, f_ecdf := cume_dist(f_value)]
  r_f_dt2[, f_label := cut(f_value, f_breaks, labels = text_f_label, ordered_result = TRUE)]
  r_f_dt2[, r_ecdf := cume_dist(r_value)]
  r_f_dt2[, r_label := cut(r_ecdf, r_breaks, labels = text_r_label, right = FALSE, ordered_result = TRUE)]
  r_f_dt2 <- r_f_dt2[, .(customer_id, f_value, f_ecdf, f_label, r_value, r_ecdf, r_label)]
  # Ensure uniqueness of customer_id
  r_f_dt2 <- unique(r_f_dt2, by = "customer_id")
  
  data_by_customer <- merge(data_by_customer, r_f_dt2, by = "customer_id")
  # Maintain one row per customer after merge
  data_by_customer <- unique(data_by_customer, by = "customer_id")
  
  if (verbose) message(paste("RF labeling completed in", round(difftime(Sys.time(), start_RF, units = "secs"), 2), "seconds"))
  
  #--------------------------------------------
  # 5. CAI Calculation
  if (verbose) message("Calculating Customer Activity Index (CAI)...")
  start_CAI <- Sys.time()
  
  # Ensure we have a 'times' field in dt for the CAI calculation
  if (!("times" %in% names(dt)) && any(c("sum_transactions_by_customer", "count_transactions_by_date") %in% names(dt))) {
    # Create times field from available alternatives
    if ("sum_transactions_by_customer" %in% names(dt)) {
      dt[, times := sum_transactions_by_customer]
    } else if ("count_transactions_by_date" %in% names(dt)) {
      dt[, times := count_transactions_by_date]
    }
  }
  
  # D01 Field Detection: Ensure we have 'ni' and 'ipt' fields in transaction-level data for CAI calculation
  if (!("ni" %in% names(dt)) || !("ipt" %in% names(dt))) {
    # Get both ni and ipt values from the customer-level data we already created
    customer_metrics <- as.data.table(df_sales_by_customer_id)[, .(customer_id, ni, ipt)]
    dt <- merge(dt, customer_metrics, by = "customer_id", all.x = TRUE, suffixes = c("", "_customer"))
    if (verbose) message("Added 'ni' and 'ipt' fields to transaction data for CAI calculation")
  }
  
  # Now perform the CAI calculation
  cai_dt <- dt[times != 1 & ni >= ni_threshold, .(
    mle = sum(ipt * (1/(ni-1))),
    wmle = sum(ipt * ((times-1) / sum(times-1)))
  ), by = customer_id]
  
  if (nrow(cai_dt) > 0) {
    cai_dt[, cai := (mle - wmle) / mle]
    cai_dt[, cai_ecdf := ecdf(cai)(cai)]
    cai_dt[, cai_label := cut(cai_ecdf, breaks = cai_breaks, labels = text_cai_label, right = FALSE, ordered_result = TRUE)]
  } else {
    cai_dt[, cai := NA]
    cai_dt[, cai_ecdf := NA]
    cai_dt[, cai_label := NA]
  }
  
  # Ensure cai_dt has unique customer_id before joining
  cai_dt <- unique(cai_dt, by = "customer_id")
  data_by_customer <- left_join_remove_duplicate2(data_by_customer, cai_dt)
  # Maintain one row per customer
  data_by_customer <- unique(data_by_customer, by = "customer_id")
  
  if (verbose) message(paste("CAI analysis completed in", round(difftime(Sys.time(), start_CAI, units = "secs"), 2), "seconds"))
  
  #--------------------------------------------
  # 6. PCV Calculation
  if (verbose) message("Calculating Past Customer Value (PCV)...")
  start_PCV <- Sys.time()
  
  # Use data.table::copy directly to avoid inheritance method issues
  pcv_dt <- dt_copy(dt)
  pcv_dt[, difftime := as.numeric(difftime(time_now, payment_time, units = "days"))]
  pcv_dt <- pcv_dt[, .(pcv = sum(total_spent * (1+delta)^(difftime))), by = customer_id]
  
  # Ensure pcv_dt has unique customer_id
  pcv_dt <- unique(pcv_dt, by = "customer_id")
  data_by_customer <- left_join_remove_duplicate2(data_by_customer, pcv_dt)
  # Maintain one row per customer
  data_by_customer <- unique(data_by_customer, by = "customer_id")
  
  if (verbose) message(paste("PCV calculation completed in", round(difftime(Sys.time(), start_PCV, units = "secs"), 2), "seconds"))
  
  #--------------------------------------------
  # 7. CLV Calculation
  if (verbose) message("Calculating Customer Lifetime Value (CLV)...")
  start_CLV <- Sys.time()
  
  pif <- function(t) {
    ifelse(t < 5, (4*t^2+20), 120 + 80*(1 - exp(5-t)))/20
  }
  
  clv_fcn <- function(x) {
    t <- 0:10
    discount_factors <- (0.9)^t / (1 + delta*4)^t
    pif_values <- pif(t)
    sapply(x, function(total_spent) sum(total_spent * pif_values * discount_factors))
  }
  
  clv_dt <- dt[, .(total_sum = sum(total_spent, na.rm = TRUE)), by = customer_id]
  clv_dt[, clv := clv_fcn(total_sum)]
  
  # Ensure clv_dt has unique customer_id
  clv_dt <- unique(clv_dt, by = "customer_id")
  data_by_customer <- merge(data_by_customer, clv_dt, by = "customer_id", all.x = TRUE)
  # Maintain one row per customer
  data_by_customer <- unique(data_by_customer, by = "customer_id")
  
  # First purchase info - ensure we have times field
  if (!("times" %in% names(dt)) && "count_transactions_by_date" %in% names(dt)) {
    dt[, times := count_transactions_by_date]
  } else if (!("times" %in% names(dt)) && "sum_transactions_by_customer" %in% names(dt)) {
    dt[, times := sum_transactions_by_customer]
  }
  
  # Now extract first purchase info
  customer_first <- dt[times == 1, .(time_first = payment_time, nt = total_spent), by = customer_id]
  customer_first[, time_first_to_now := as.numeric(difftime(time_now, time_first, units = "days"))]
  # Ensure customer_first has unique customer_id
  customer_first <- unique(customer_first, by = "customer_id")
  data_by_customer <- merge(data_by_customer, customer_first, by = "customer_id", all.x = TRUE)
  # Maintain one row per customer
  data_by_customer <- unique(data_by_customer, by = "customer_id")
  
  if (verbose) message(paste("CLV calculation completed in", round(difftime(Sys.time(), start_CLV, units = "secs"), 2), "seconds"))
  
  #--------------------------------------------
  # 8. E0T Calculation (Average purchase amount for customers in E0 segment)
  if (verbose) message("Calculating e0t (core customer purchase values)...")
  start_e0t <- Sys.time()
  
  e0t_dt <- merge(dt, ipt_table, by = "customer_id", all.x = TRUE)
  e0t_dt[, ipt_mean := as.difftime(ipt_mean, units = "days")]
  e0t_dt[, difftime := as.numeric(difftime(time_now, payment_time, units = "days"))]
  e0t_dt[, nes_ratio := difftime / as.numeric(ipt_mean)]
  e0t_dt[, nes := cut(nes_ratio, breaks = nes_breaks * nes_median, labels = text_nes_label, right = FALSE, ordered_result = TRUE)]
  e0t_dt2 <- e0t_dt[nes == "E0", .(e0t = mean(total_spent, na.rm = TRUE)), by = customer_id]
  e0t_dt2 <- e0t_dt2[!is.na(customer_id)]
  
  # Ensure e0t_dt2 has unique customer_id
  e0t_dt2 <- unique(e0t_dt2, by = "customer_id")
  data_by_customer <- left_join_remove_duplicate2(data_by_customer, e0t_dt2)
  # Maintain one row per customer
  data_by_customer <- unique(data_by_customer, by = "customer_id")
  
  if (verbose) message(paste("e0t calculation completed in", round(difftime(Sys.time(), start_e0t, units = "secs"), 2), "seconds"))
  
  #--------------------------------------------
  # 9. CRI Calculation (Customer Regularity Index)
  if (verbose) message("Calculating CRI (Customer Regularity Index)...")
  start_CRI <- Sys.time()
  
  cri_dt <- ipt_table[ni>1]
  
  # Calculate alpha and N
  alpha <- mean(cri_dt$ni, na.rm = TRUE)
  n <- nrow(cri_dt)
  
  # Calculate inverse of ipt_mean (safely)
  cri_dt[, ipt_mean_inv := ifelse(ipt_mean > 0, 1 / ipt_mean, NA)]
  
  # Calculate theta, handle NA values
  theta <- sum(cri_dt$ipt_mean_inv, na.rm = TRUE) / (alpha * n)
  
  # Prevent division by zero or negative numbers
  if ((alpha - 1) <= 0 || is.na(theta) || theta == 0) {
    ge <- NA
  } else {
    ge <- 1 / ((alpha - 1) * theta)
  }
  
  # Calculate be and cri (note: if ge is NA, results will be NA)
  cri_table <- cri_dt[, .(
    ge = ge,
    ie = ipt_mean,
    be = (ni / (ni + alpha - 1)) * ipt_mean + ((alpha - 1) / (ni + alpha - 1)) * ge,
    cri = abs(ipt_mean - ((ni / (ni + alpha - 1)) * ipt_mean + ((alpha - 1) / (ni + alpha - 1)) * ge)) / abs(ipt_mean - ge)
  ), by = customer_id]
  
  # Calculate cri_ecdf: if the vector length is 0, set to NA
  cri_vals <- abs(cri_dt$ipt_mean - ((cri_dt$ni / (cri_dt$ni + alpha - 1)) * cri_dt$ipt_mean + ((alpha - 1) / (cri_dt$ni + alpha - 1)) * ge)) / abs(cri_dt$ipt_mean - ge)
  if (length(na.omit(cri_vals)) > 0) {
    ecdf_func <- ecdf(cri_vals)
    cri_table[, cri_ecdf := ecdf_func(cri)]
  } else {
    cri_table[, cri_ecdf := NA]
  }
  
  # Ensure cri_table has unique customer_id
  cri_table <- unique(cri_table, by = "customer_id")
  data_by_customer <- left_join_remove_duplicate2(data_by_customer, cri_table)
  # Maintain one row per customer
  data_by_customer <- unique(data_by_customer, by = "customer_id")
  
  # Fix for be2_table
  be2_table <- ipt_table[, .(
    be2 = ifelse(is.na((ni / (ni + alpha - 1)) * ipt_mean + ((alpha - 1) / (ni + alpha - 1)) * ge),
               ge,
               (ni / (ni + alpha - 1)) * ipt_mean + ((alpha - 1) / (ni + alpha - 1)) * ge)
  ), by = customer_id]
  
  # Ensure be2_table has unique customer_id
  be2_table <- unique(be2_table, by = "customer_id")
  data_by_customer <- left_join_remove_duplicate2(data_by_customer, be2_table)
  # Maintain one row per customer
  data_by_customer <- unique(data_by_customer, by = "customer_id")
  
  if (verbose) message(paste("CRI calculation completed in", round(difftime(Sys.time(), start_CRI, units = "secs"), 2), "seconds"))
  
  #--------------------------------------------
  # 10. Nrec Calculation (Churn prediction)
  if (verbose) message("Calculating churn prediction (nrec)...")
  start_nrec <- Sys.time()
  
  trace_month <- -3
  date_rows <- dt
  date_row_ingroupt <- date_rows[payment_time < add_with_rollback(time_now, months(trace_month))]
  
  if(nrow(date_row_ingroupt) == 0) {
    data_by_customer[, `:=`(nrec_prob = 1, nrec = "rec")]
    nrec_accu <- data.frame(nrec_accu = "Accuracy: 100%")
  } else {
    # Calculate ncode: determine if each record occurred after time_now - trace_month
    date_rows[, ncode := (payment_time > add_with_rollback(time_now, months(trace_month)))]
    
    # For each customer_id, get record with max times, calculate nrec = 1 - max(ncode)
    idx <- date_rows[, .I[which.max(times)], by = customer_id]$V1
    date_row_nrec <- date_rows[idx]
    date_row_nrec <- date_row_nrec[, .(customer_id, nrec = 1 - max(ncode)), by = customer_id]
    
    # For each customer_id in date_row_ingroupt, get most recent purchase record
    nrec_ingroup <- date_row_ingroupt[date_row_ingroupt[, .I[which.max(times)], by = customer_id]$V1, 
                                   .(customer_id, times, payment_time)]
    nrec_ingroup2 <- dt_copy(nrec_ingroup)
    nrec_ingroup2[, f_value := times]
    nrec_ingroup2[, f_ecdf := cume_dist(f_value)]
    nrec_ingroup2[, f_label := cut(f_value, f_breaks, labels = text_f_label, ordered_result = TRUE)]
    # Keep only customer_id and f_label as predictor variables
    nrec_ingroup2 <- nrec_ingroup2[, .(customer_id, f_label)]
    
    # Merge nrec_ingroup2 and date_row_nrec to form training data da1
    da1 <- merge(
      as.data.frame(nrec_ingroup2)[, c("customer_id", "f_label")],
      as.data.frame(date_row_nrec)[, c("customer_id", "nrec")],
      by = "customer_id", 
      all.x = TRUE
    )
    
    # Convert nrec to factor, 0 mapped to "nrec", 1 mapped to "rec"
    da1$nrec <- factor(da1$nrec, levels = c(0, 1), labels = c("nrec", "rec"))
    
    # Get cai field from data_by_customer, merge into da1 (using customer_id as key)
    if("cai" %in% names(data_by_customer)) {
      da <- merge(da1, as.data.frame(data_by_customer[, c("customer_id", "cai")]), by = "customer_id", all.x = TRUE)
    } else {
      da <- da1
      da$cai <- NA
    }
    
    # Check if training data contains required variables
    required_vars <- c("nrec", "f_label", "cai")
    missing_vars <- setdiff(required_vars, names(da))
    if(length(missing_vars) > 0) {
      if (verbose) message("Missing variables for churn model: ", paste(missing_vars, collapse = ", "))
      data_by_customer[, `:=`(nrec_prob = NA, nrec = NA)]
      nrec_accu <- data.frame(nrec_accu = NA)
    } else {
      # Remove rows with NA (for model training)
      da_clean <- na.omit(da[, required_vars])
      
      if (nrow(da_clean) < 10) {
        if (verbose) message("Insufficient data for churn model. Need at least 10 complete observations.")
        data_by_customer[, `:=`(nrec_prob = NA, nrec = NA)]
        nrec_accu <- data.frame(nrec_accu = NA)
      } else if (skip_within_subject) {
        # Skip modeling if requested
        if (verbose) message("Skipping within-subject churn modeling as requested.")
        data_by_customer[, `:=`(nrec_prob = 0.5, nrec = "unknown")]
        nrec_accu <- data.frame(nrec_accu = "Accuracy: N/A (skipped)")
      } else {
        # Train a logistic regression model with cross-validation
        cv_control <- trainControl(method = "cv", number = 10, savePredictions = "final", classProbs = FALSE)
        logistic_model_cv <- train(nrec ~ f_label + cai, data = da_clean, method = "glm", family = "binomial", trControl = cv_control)
        
        # Evaluate model accuracy
        predictions <- logistic_model_cv$pred
        predicted_class <- predictions$pred
        observed_class <- predictions$obs
        cm <- confusionMatrix(predicted_class, observed_class)
        accuracy <- cm$overall["Accuracy"]
        nrec_accu <- paste0("Accuracy: ", round(accuracy * 100, 2), "%")
        
        # Create prediction data: merge nrec_ingroup2 and date_row_nrec
        nrec_add <- merge(
          as.data.frame(nrec_ingroup2)[, c("customer_id", "f_label")],
          as.data.frame(date_row_nrec)[, c("customer_id", "nrec")],
          by = "customer_id", all.x = TRUE
        )
        
        if("cai" %in% names(data_by_customer)) {
          nrec_add <- merge(nrec_add, as.data.frame(data_by_customer[, c("customer_id", "cai")]), by = "customer_id", all.x = TRUE)
        } else {
          nrec_add$cai <- NA
        }
        
        # Initialize prediction vectors for all rows
        n_all <- nrow(nrec_add)
        pred_numeric_full <- rep(NA, n_all)
        pred_prob_full <- rep(NA, n_all)
        
        # Create a complete case indicator column
        nrec_add <- nrec_add %>% mutate(complete_case = !is.na(f_label) & !is.na(cai))
        
        # If there are complete cases, predict for these rows
        if(sum(nrec_add$complete_case) > 0) {
          complete_idx <- which(nrec_add$complete_case)
          preds_raw <- predict(logistic_model_cv, newdata = nrec_add[complete_idx, ], type = "raw")
          preds_prob <- predict(logistic_model_cv, newdata = nrec_add[complete_idx, ], type = "prob")[[2]]
          
          # Store prediction results in full-length vectors
          pred_numeric_full[complete_idx] <- as.numeric(as.character(preds_raw))
          pred_prob_full[complete_idx] <- preds_prob
        }
        
        # Calculate group mean probability (using only complete case predictions)
        group_mean_prob <- mean(pred_prob_full, na.rm = TRUE)
        
        # Fill in predictions based on complete_case
        nrec_add <- nrec_add %>%
          mutate(
            nrec_prob = ifelse(!complete_case, group_mean_prob, pred_prob_full),
            nrec = ifelse(!complete_case,
                       ifelse(group_mean_prob > 0.5, "rec", "nrec"),
                       ifelse(pred_numeric_full == 1, "rec", "nrec"))
          ) %>%
          select(-complete_case)  # Remove helper column
        
        # Ensure nrec_add has unique customer_id
        nrec_add <- unique(nrec_add, by = "customer_id")
        # Merge prediction results back to data_by_customer
        data_by_customer <- left_join_remove_duplicate2(data_by_customer, nrec_add)
        # Maintain one row per customer
        data_by_customer <- unique(data_by_customer, by = "customer_id")
      }
      
      nrec_accu <- data.frame(nrec_accu = nrec_accu)
    }
    
    if (verbose) message(paste("Churn prediction completed in", round(difftime(Sys.time(), start_nrec, units = "secs"), 2), "seconds"))
  }
  
  total_time <- difftime(Sys.time(), total_start_time, units = "secs")
  if (verbose) message(paste("Customer DNA analysis completed in", round(total_time, 2), "seconds"))
  
  # Final check to ensure the result has one row per customer_id
  data_by_customer <- unique(data_by_customer, by = "customer_id")
  
  # Clean up any remaining duplicate column names before returning
  data_by_customer <- clean_colnames(data_by_customer)
  
  # Find and fix any remaining duplicate column names (more aggressive approach)
  
  data_by_customer <- clean_colnames(data_by_customer)
  col_names <- names(data_by_customer)
  dup_cols <- col_names[duplicated(col_names)]
  
  if (length(dup_cols) > 0) {
    if (verbose) message("Handling remaining duplicate columns: ", paste(dup_cols, collapse=", "))
    
    for (col in unique(dup_cols)) {
      # Get all indices of this column name
      indices <- which(col_names == col)
      
      # Keep the first occurrence, rename others
      for (i in 2:length(indices)) {
        new_name <- paste0(col, "_", i)
        if (verbose) message("Renaming duplicate column '", col, "' to '", new_name, "'")
        setnames(data_by_customer, indices[i], new_name)
      }
    }
  }
  
  if (verbose) {
    n_customers <- nrow(data_by_customer)
    n_unique_customers <- uniqueN(data_by_customer$customer_id)
    
    if (n_customers != n_unique_customers) {
      warning("WARNING: Output contains ", n_customers, " rows but only ", n_unique_customers, " unique customer_ids. Taking unique values.")
      data_by_customer <- unique(data_by_customer, by = "customer_id")
    } else {
      message("Verified: Output contains exactly one row per customer (",n_customers," customers in total)")
    }
  }

  
  # Make sure there are no duplicate column names before converting to tibble
  # Create a unique name repair function
  fix_names <- function(names) {
    if (anyDuplicated(names)) {
      for (i in seq_along(names)) {
        count <- sum(names[1:i] == names[i])
        if (count > 1) {
          names[i] <- paste0(names[i], "_", count)
        }
      }
    }
    return(names)
  }
  
  # Apply the name repair to ensure as_tibble works correctly
  setnames(data_by_customer, fix_names(names(data_by_customer)))

  # Standardize column names for UI compatibility
  if ("cai" %in% names(data_by_customer)) {
    setnames(data_by_customer, "cai", "cai_value")
  }
  
  # Create nes_value as alias for nes_ratio for UI compatibility
  if ("nes_ratio" %in% names(data_by_customer)) {
    data_by_customer[, nes_value := nes_ratio]
  }

  return(list(data_by_customer = as_tibble(data_by_customer), nrec_accu = as_tibble(nrec_accu)))
}
