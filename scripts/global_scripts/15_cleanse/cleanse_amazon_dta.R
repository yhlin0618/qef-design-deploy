#' Cleanse Amazon Sales Data
#'
#' This function cleanses raw Amazon sales data and prepares it for DNA analysis without
#' requiring the product_property_dictionary table. It performs basic data cleaning,
#' transformations, and validation needed for customer DNA analysis.
#'
#' @param raw_data DBI connection. Connection to the database containing raw sales data.
#' @param cleansed_data DBI connection. Connection to the database where cleansed data will be stored.
#' @param verbose Logical. Whether to display progress messages. Default is TRUE.
#'
#' @return Invisibly returns the cleansed_data connection for chaining.
#'
#' @details
#' This function performs the following operations:
#' 1. Identifies customer identifier (email or another field)
#' 2. Extracts customer_id from available identifier
#' 3. Renames columns for consistency
#' 4. Filters for US-only orders if country data is available
#' 5. Writes the cleansed data to the destination database
#'
#' @export
cleanse_amazon_dta <- function(raw_data, cleansed_data, verbose = TRUE) {
  # Load required packages
  library(dplyr)
  library(dbplyr)
  library(stringr)
  
  # Input validation
  if (!inherits(raw_data, "DBIConnection")) {
    stop("raw_data must be a DBI database connection")
  }
  
  if (!inherits(cleansed_data, "DBIConnection")) {
    stop("cleansed_data must be a DBI database connection")
  }
  
  # Check if raw table exists
  if (!DBI::dbExistsTable(raw_data, "amazon_sales_dta")) {
    stop("Table 'amazon_sales_dta' does not exist in the raw_data database")
  }
  
  if (verbose) message("Cleansing Amazon sales data...")
  
  # Process data
  tryCatch({
    # Reference raw table
    amazon_sales_dta <- tbl(raw_data, "amazon_sales_dta")
    
    # Get column names to check what's available
    cols <- colnames(collect(head(amazon_sales_dta, 1)))
    
    if (verbose) {
      message("Available columns in amazon_sales_dta:")
      print(cols)
    }
    
    # Look for various possible customer identifier columns
    customer_id_cols <- c("buyer_email", "customer_email", "email", "customer_id", "buyer_id")
    available_id_cols <- intersect(customer_id_cols, cols)
    
    if (length(available_id_cols) == 0) {
      # If no email column is found, look for order identifiers
      order_id_cols <- c("order_id", "amazon_order_id", "merchant_order_id")
      available_order_cols <- intersect(order_id_cols, cols)
      
      if (length(available_order_cols) > 0) {
        if (verbose) message("No email column found. Using ", available_order_cols[1], " as customer identifier.")
        id_col <- available_order_cols[1]
        has_email <- FALSE
      } else {
        stop("No suitable customer identifier column found. Need one of: ", 
             paste(c(customer_id_cols, order_id_cols), collapse=", "))
      }
    } else {
      # Use the first available email column
      id_col <- available_id_cols[1]
      has_email <- TRUE
      if (verbose) message("Using ", id_col, " as customer identifier.")
    }
    
    # Check for purchase date column
    date_cols <- c("purchase_date", "order_date", "time", "date")
    available_date_cols <- intersect(date_cols, cols)
    
    if (length(available_date_cols) == 0) {
      stop("No purchase date column found. Need one of: ", paste(date_cols, collapse=", "))
    } else {
      date_col <- available_date_cols[1]
      if (verbose) message("Using ", date_col, " as purchase date.")
    }
    
    # Check for product identifier
    product_cols <- c("sku", "asin", "product_id", "product_id")
    available_product_cols <- intersect(product_cols, cols)
    
    if (length(available_product_cols) == 0) {
      stop("No product identifier column found. Need one of: ", paste(product_cols, collapse=", "))
    } else {
      product_col <- available_product_cols[1]
      if (verbose) message("Using ", product_col, " as product identifier.")
    }
    
    # Define price column based on what's available
    price_cols <- c("product_price", "price", "unit_price", "total_price")
    available_price_cols <- intersect(price_cols, cols)
    
    if (length(available_price_cols) > 0) {
      price_col <- available_price_cols[1]
      has_price <- TRUE
      if (verbose) message("Using ", price_col, " as price.")
    } else {
      has_price <- FALSE
      if (verbose) message("No price column found. Price data will not be available.")
    }
    
    # Define postal code columns based on what's available
    postal_cols <- c("shipping_postal_code", "postal_code", "zip_code", "zip")
    available_postal_cols <- intersect(postal_cols, cols)
    
    if (length(available_postal_cols) > 0) {
      postal_col <- available_postal_cols[1]
      has_postal <- TRUE
      if (verbose) message("Using ", postal_col, " as postal code.")
    } else {
      has_postal <- FALSE
      if (verbose) message("No postal code column found. Location data will not be available.")
    }
    
    # Process data - first create base query
    query <- amazon_sales_dta
    
    # Apply filters based on available columns
    if (has_email) {
      # Only filter for valid emails if using an email column
      query <- query %>% filter(str_detect(!!sym(id_col), "@"))
      
      # Extract customer ID from email address
      query <- query %>% mutate(
        customer_id = sql(paste0("LOWER(SUBSTR(", id_col, ", 1, POSITION('@' IN ", id_col, ") - 1)"))
      )
    } else {
      # Use the identifier directly if not an email
      query <- query %>% mutate(customer_id = !!sym(id_col))
    }
    
    # Add time column from the identified date column
    query <- query %>% mutate(time = !!sym(date_col))
    
    # Rename columns if they exist
    if (has_price) {
      query <- query %>% rename(lineproduct_price = !!sym(price_col))
    }
    
    if (has_postal) {
      query <- query %>% rename(zip_code = !!sym(postal_col))
    }
    
    # Rename product identifier if needed
    if (product_col != "sku") {
      query <- query %>% rename(sku = !!sym(product_col))
    }
    
    # Filter for required fields
    query <- query %>% filter(!is.na(customer_id) & !is.na(time) & !is.na(sku))
    
    # Filter for US only if country code exists
    if ("shipping_country_code" %in% cols) {
      query <- query %>% filter(shipping_country_code == "US")
    }
    
    # Select and order columns
    if (has_price) {
      query <- query %>% select(customer_id, time, sku, lineproduct_price, everything())
    } else {
      query <- query %>% select(customer_id, time, sku, everything())
    }
    
    # Collect the results
    if (verbose) message("Executing query to collect data...")
    
    # Try to collect with error handling
    tryCatch({
      cleansed_data_df <- query %>% collect()
      
      # Check if we got any data
      if (nrow(cleansed_data_df) == 0) {
        warning("No data returned after applying filters. Please check your data source and filters.")
      }
    }, error = function(e) {
      # If collection fails, try a simpler approach
      if (verbose) message("Collection failed. Trying simplified approach...")
      
      # Get raw data first, then process in memory
      raw_data_df <- tbl(raw_data, "amazon_sales_dta") %>% collect()
      
      if (verbose) message("Raw data collected. Processing locally...")
      
      # Process the data locally
      cleansed_data_df <<- raw_data_df %>%
        mutate(
          customer_id = if (has_email) {
            ifelse(str_detect(!!sym(id_col), "@"),
                  str_extract(!!sym(id_col), "^[^@]+"),
                  !!sym(id_col))
          } else {
            !!sym(id_col)
          },
          time = if (date_col %in% names(raw_data_df)) as.POSIXct(raw_data_df[[date_col]]) else Sys.time()
        ) %>%
        filter(!is.na(customer_id) & !is.na(time))
      
      if (product_col != "sku" && product_col %in% names(raw_data_df)) {
        cleansed_data_df$sku <- raw_data_df[[product_col]]
      } else if (!"sku" %in% names(cleansed_data_df)) {
        cleansed_data_df$sku <- paste0("SKU", sprintf("%04d", 1:nrow(cleansed_data_df)))
      }
      
      # Add price column if available
      if (has_price && price_col %in% names(raw_data_df)) {
        cleansed_data_df$lineproduct_price <- raw_data_df[[price_col]]
      }
      
      # Add postal column if available
      if (has_postal && postal_col %in% names(raw_data_df)) {
        cleansed_data_df$zip_code <- raw_data_df[[postal_col]]
      }
    })
    
    # Ensure we have all required columns
    if (!"lineproduct_price" %in% names(cleansed_data_df)) {
      if (verbose) message("Adding default lineproduct_price column...")
      cleansed_data_df$lineproduct_price <- 1
    }
    
    # Write to destination database
    if (verbose) message("Writing data to cleansed database...")
    DBI::dbWriteTable(cleansed_data, "amazon_sales_dta", cleansed_data_df, overwrite = TRUE, temporary = FALSE)
    
    if (verbose) {
      message("Amazon sales data cleansing complete:")
      message("  - Cleansed rows: ", nrow(cleansed_data_df))
      message("  - Data written to 'amazon_sales_dta' table in cleansed database")
    }
    
  }, error = function(e) {
    stop("Error cleansing Amazon sales data: ", e$message)
  })
  
  # Return connection for chaining
  invisible(cleansed_data)
}