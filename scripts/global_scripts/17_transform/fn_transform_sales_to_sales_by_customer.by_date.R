transform_sales_to_sales_by_customer.by_date <- function(df, 
                                                            first_cols = c("zipcode", "state", "lat", "lng"),
                                                            time = "payment_time",
                                                            verbose = TRUE) {
  #' Transform Sales Data into Customer-Date Aggregated Table by Time Field (Vectorized)
  #'
  #' @description
  #' This function aggregates raw sales data by customer_id and transaction date
  #' (derived from the time field, which is specified by the argument `time`, by truncating to day),
  #' calculating daily sum of sales amounts, count of transactions, and the first record of other specified fields.
  #' It always includes the earliest value of the time field from each group so that the day difference between orders
  #' (ipt - Inter-Purchase Time) can be computed. It also generates each customer's total purchase count and order sequence.
  #'
  #' @param df A data.table containing raw sales data with the following required fields:
  #'   - customer_id   : Customer identifier (required)
  #'   - <time>        : Time field specified by the argument `time` (required, used to generate date and calculate order intervals)
  #'                     Supports "payment_time" (default) or "min_time_by_date" as alternatives
  #'   - total_spent   : Individual sale amount (required)
  #' In addition to the above fields, other fields such as zipcode, state, lat, lng can be provided as needed.
  #' The first_cols parameter specifies additional fields (other than the time field) for which the first record
  #' in each group should be taken. If a specified field does not exist in the input data, it will not be included in the output.
  #'
  #' @param first_cols A character vector specifying additional fields to include in the grouped result by taking
  #' the first record value. Default is c("zipcode", "state", "lat", "lng"). Note: the time field specified by `time`
  #' is always included.
  #'
  #' @param time A character string indicating the name of the time field to use for aggregation. Default is "payment_time".
  #'             Function also supports "min_time_by_date" as an alternative time field name.
  #'
  #' @param verbose Logical; whether to display processing progress and status information. Default is TRUE.
  #'
  #' @return Returns a data.table with the following columns:
  #'   - customer_id   : Customer identifier
  #'   - date          : Transaction date (derived from the time field truncated to day)
  #'   - sum_spent_by_date        : Daily sum of sales amounts (sum of all total_spent for that day)
  #'   - count_transactions_by_date: Number of transactions for that day (count)
  #'   - min_time_by_date: The earliest value of the time field from each group (used for ipt calculation)
  #'   - min_time      : The minimum time across all dates for each customer (min of min_time_by_date)
  #'   - Other fields specified in first_cols that exist in the input data: first record for each group
  #'   - ni            : Customer's total purchase count (aggregated by customer_id)
  #'   - times         : Customer's order sequence (numbered according to time order)
  #'   - ipt           : Days between orders (Inter-Purchase Time), calculated from differences in the time field
  #'
  #' @examples
  #' # Assuming sales_df is raw sales data with required fields and additional zipcode and state fields:
  #' result <- transform_sales_to_sales_by_customer_id.by_date(sales_df, first_cols = c("zipcode", "state"), time = "payment_time")
  
  start_time_overall <- Sys.time()
  if (verbose) message("Starting sales data transformation...")
  
  # Check required fields (using the provided time argument)
  req_cols <- c("customer_id", time, "total_spent")
  # Support alternative field names
  if ("lineproduct_price" %in% names(df) && !"total_spent" %in% names(df)) {
    if (verbose) message("Field 'lineproduct_price' found instead of 'total_spent'. Using 'lineproduct_price'.")
    setnames(df, "lineproduct_price", "total_spent")
  }
  
  # Support alternative time field names
  if (time == "payment_time" && !"payment_time" %in% names(df) && "min_time_by_date" %in% names(df)) {
    if (verbose) message("Field 'min_time_by_date' found instead of 'payment_time'. Using 'min_time_by_date'.")
    time <- "min_time_by_date"
  }
  
  # Check again after field name adjustments
  req_cols <- c("customer_id", time, "total_spent")
  missing_req <- setdiff(req_cols, names(df))
  if (length(missing_req) > 0) {
    stop(paste("Missing required fields:", paste(missing_req, collapse = ", ")))
  }
  
  require(data.table)
  require(lubridate)
  
  # Convert input to data.table if not already
  if (!is.data.table(df)) {
    if (verbose) message("Converting input data to data.table format...")
    df <- as.data.table(df)
  }
  
  # Display basic input information using the time field specified
  if (verbose) {
    message(sprintf("Input data contains %d records, %d fields", nrow(df), ncol(df)))
    message(sprintf("Date range: %s to %s", 
                    format(min(df[[time]], na.rm = TRUE), "%Y-%m-%d"),
                    format(max(df[[time]], na.rm = TRUE), "%Y-%m-%d")))
    message(sprintf("Contains %d unique customers", length(unique(df$customer_id))))
  }
  
  # Generate date field from the time field (truncate to day)
  if (verbose) message(sprintf("Generating date field from %s...", time))
  df[, date := floor_date(get(time), unit = "day")]
  
  if (verbose) message(sprintf("Will take first record for the following additional fields: %s", 
                               paste(first_cols, collapse = ", ")))
  
  # Determine which additional fields from first_cols exist in the data
  extra_cols <- intersect(first_cols, names(df))
  setorder(df,customer_id,date )
  # Aggregate by customer_id and date following R50 (data.table Vectorized Operations)
  # Use more concise and efficient data.table syntax
  if (verbose) message("Aggregating by customer ID and date (vectorized)...")
  result <- df[, c(
    .(
      sum_spent_by_date = sum(total_spent, na.rm = TRUE),
      count_transactions_by_date = .N,
      min_time_by_date = min(get(time))  # Use min() to ensure the earliest time is selected
    ),
    # Process extra columns using lapply on .SD (R49 & R50 principles)
    if (length(extra_cols) > 0) {
      lapply(.SD, first)  # Take first value for each additional column
    }
  ), by = .(customer_id, date), .SDcols = extra_cols]
  
  # Calculate customer's total purchase count (ni) and order sequence (times)
  if (verbose) message("Calculating customer purchase counts and order sequence...")
  result[, `:=`(
    ni = .N,
    times = seq_len(.N)
  ), by = customer_id]
  
  # Calculate ipt using the aggregated time field (which comes from the field specified by time parameter)
  if (verbose) message("Calculating Inter-Purchase Time (ipt)...")
  result[, ipt := as.numeric(difftime(min_time_by_date, shift(min_time_by_date), units = "days")), by = customer_id]
  
  # Clean column names to prevent duplicates with .x and .y suffixes
  clean_colnames <- function(dt) {
    # Find columns ending with .x
    pattern <- "\\.x$"
    x_cols <- grep(pattern, names(dt), value = TRUE)
    
    # For each .x column, if there's a corresponding .y column, remove both and keep just the base name
    for (col in x_cols) {
      base_name <- sub("\\.x$", "", col)
      y_col <- paste0(base_name, ".y")
      
      # If both .x and .y versions exist
      if (y_col %in% names(dt)) {
        # Create a new column with just the base name
        dt[[base_name]] <- dt[[col]]
        
        # Remove the .x and .y columns
        dt[, c(col, y_col) := NULL]
      }
    }
    return(dt)
  }
  
  # Apply column name cleaning
  result <- clean_colnames(result)
  
  # Sort the result by customer_id and date
  if (verbose) message("Sorting results by customer ID and date...")
  setorder(result, customer_id, date)
  
  end_time_overall <- Sys.time()
  process_time <- difftime(end_time_overall, start_time_overall, units = "secs")
  
  if (verbose) {
    message(sprintf("Transformation complete! Took %.2f seconds", as.numeric(process_time)))
    message(sprintf("Result contains %d records, %d fields", nrow(result), ncol(result)))
    message(sprintf("Contains %d unique customers, %d unique dates", 
                    length(unique(result$customer_id)),
                    length(unique(result$date))))
  }
  
  return(result)
}
