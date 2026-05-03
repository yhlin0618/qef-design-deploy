transform_sales_by_customer.by_date_to_sales_by_customer <- function(df, 
                                                                           first_cols = c("min_time_by_date", "ni", "ipt", "zipcode", "state", "lat", "lng"),
                                                                           verbose = TRUE) {
  #' Transform Customer-Date Aggregated Sales Data to Customer-Level Data (Vectorized)
  #'
  #' @description
  #' This function aggregates sales data that has been grouped by customer ID and date into a customer-level dataset.
  #' For each customer, it calculates:
  #'   - **sum_sales_by_customer**: Overall sum of sales amounts (sum of daily sum_spent_by_date values)
  #'   - **sum_transactions_by_customer**: Overall sum of transactions (sum of daily count_transactions_by_date values)
  #' Additionally, for the fields specified in `first_cols` that exist in the input data,
  #' the function retains the first occurrence (for example, the minimum time, total purchase count, IPT,
  #' and geographic information).
  #'
  #' @param df A data.table (or data.frame convertible to data.table) that has been aggregated by customer ID and date.
  #'           The data must include the following columns:
  #'             - customer_id               : Customer identifier
  #'             - sum_spent_by_date         : Daily sum of sales amounts (from previous aggregation)
  #'             - count_transactions_by_date: Number of transactions for that day (count)
  #'           It is recommended that the following columns also be present for aggregation:
  #'             - min_time_by_date: The minimum payment time across all dates for the customer
  #'             - ni      : Customer's total purchase count (or number of purchases)
  #'             - ipt     : Inter-Purchase Time (days between orders)
  #'           Additional fields (e.g. zipcode, state, lat, lng) may also be present.
  #'
  #' @param first_cols A character vector specifying fields to include in the output.
  #'                   For each such field that exists in the input data, a single value is aggregated for the customer.
  #'                   Special handling is done for "min_time_by_date", "ni", and "ipt" (see details above).
  #'                   Default is c("min_time_by_date", "ni", "ipt", "zipcode", "state", "lat", "lng").
  #'
  #' @param verbose Logical; if TRUE, prints progress messages. Default is TRUE.
  #'
  #' @return Returns a data.table with one row per customer, containing:
  #'         - customer_id                 : Customer identifier
  #'         - sum_sales_by_customer       : Sum of sum_spent_by_date over all days (overall sales)
  #'         - sum_transactions_by_customer: Sum of count_transactions_by_date over all days (overall transaction count)
  #'         - ipt                         : First valid inter-purchase time value for the customer (first non-NA value within group)
  #'         - ni                          : Total number of purchases (maximum value from input rows)
  #'         - min_time_by_date            : The minimum (earliest) time across all dates for this customer
  #'         - max_time_by_date            : The maximum (latest) time across all dates for this customer
  #'         - Additional fields specified in first_cols (if present), taken as the first non-NA occurrence.
  #'
  #' @examples
  #' # Assuming sales_by_date is a data.table aggregated by customer_id and date:
  #' customer_df <- transform_sales_by_customer_id.by_date_to_sales_by_customer_id(sales_by_date, 
  #'                          first_cols = c("min_time_by_date", "ni", "ipt", "zipcode", "state", "lat", "lng"))
  
  start_time <- Sys.time()
  if (verbose) message("Starting transformation to customer-level data...")
  
  require(data.table)
  
  # Ensure input is a data.table
  if (!is.data.table(df)) {
    if (verbose) message("Converting input data to data.table format...")
    df <- as.data.table(df)
  }
  
  # Check that required columns exist in the aggregated data
  req_cols <- c("customer_id", "sum_spent_by_date", "count_transactions_by_date")
  missing_req <- setdiff(req_cols, names(df))
  if (length(missing_req) > 0) {
    stop(paste("Missing required fields in the aggregated data:", paste(missing_req, collapse = ", ")))
  }
  
  # Warn if important columns for first_cols are missing
  important_cols <- c("min_time_by_date", "ni", "ipt")
  missing_important <- setdiff(important_cols, names(df))
  if (length(missing_important) > 0 && verbose) {
    warning("Some important columns are missing from input data: ", paste(missing_important, collapse = ", "), 
            ". These should normally be included.")
  }
  
  # Determine extra columns to aggregate (excluding special ones)
  extra_cols <- setdiff(first_cols, c("min_time_by_date", "ni", "ipt"))
  # Only include those that exist in df
  extra_cols <- intersect(extra_cols, names(df))
  
  if (verbose) message("Aggregating customer-level metrics (vectorized)...")
  
  result <- df[, c(
    .( sum_sales_by_customer        = sum(sum_spent_by_date, na.rm = TRUE),
       sum_transactions_by_customer = sum(count_transactions_by_date, na.rm = TRUE),
       ipt  = first(na.omit(ipt)),
       ni   = if ("ni" %in% names(df)) max(ni, na.rm = TRUE) else NA_real_,
       min_time_by_date = if ("min_time_by_date" %in% names(df)) min(min_time_by_date, na.rm = TRUE) else NA,
       max_time_by_date = if ("min_time_by_date" %in% names(df)) max(min_time_by_date, na.rm = TRUE) else NA  # Added for correct ipt_mean calculation
    ),
    lapply(.SD, function(x) first(na.omit(x)))
  ), by = customer_id, .SDcols = extra_cols]
  
  if (verbose) {
    message(sprintf("Customer-level aggregation complete. %d unique customers found.", nrow(result)))
    message(sprintf("Transformation took %.2f seconds.", as.numeric(difftime(Sys.time(), start_time, units = "secs"))))
  }
  
  return(result)
}
