#' Create Sample Amazon Sales Data
#'
#' This function creates sample Amazon sales data for testing DNA analysis.
#' It generates synthetic data that mimics the structure of Amazon sales data
#' and loads it into the specified database.
#'
#' IMPORTANT: This function is for DEVELOPMENT and TESTING environments ONLY.
#' It should NEVER be used in production environments, as it violates MP29 (No Fake Data).
#'
#' @param target_db DBI connection. Connection to the database where sample data will be stored.
#' @param num_customers Integer. Number of unique customers to generate. Default is 100.
#' @param min_purchases Integer. Minimum number of purchases per customer. Default is 1.
#' @param max_purchases Integer. Maximum number of purchases per customer. Default is 5.
#' @param start_date Date. Start date for purchase dates. Default is 1 year ago.
#' @param end_date Date. End date for purchase dates. Default is current date.
#' @param overwrite Logical. Whether to overwrite existing table. Default is TRUE.
#' @param verbose Logical. Whether to display detailed progress. Default is TRUE.
#'
#' @return Invisibly returns the target_db connection for chaining.
#'
#' @details
#' The sample data includes the following columns:
#' - customer_id: Unique identifier for each customer
#' - time: Purchase timestamp
#' - sku: Product SKU
#' - lineproduct_price: Price of the product
#' - order_id: Unique order identifier
#' - quantity: Number of products purchased
#'
#' @export
create_sample_amazon_data <- function(target_db, num_customers = 100, 
                                     min_purchases = 1, max_purchases = 5,
                                     start_date = Sys.Date() - 365, 
                                     end_date = Sys.Date(),
                                     overwrite = TRUE, verbose = TRUE) {
  # Check if this is a production environment
  is_production <- function() {
    # Check for an environment variable indicating production
    prod_env <- Sys.getenv("PRECISION_ENV", unset = NA)
    if (!is.na(prod_env) && prod_env == "production") {
      return(TRUE)
    }
    
    # Check for existence of OPERATION_MODE in global environment
    if (exists("OPERATION_MODE", envir = .GlobalEnv)) {
      mode_val <- get("OPERATION_MODE", envir = .GlobalEnv)
      if (mode_val == "APP_MODE" || mode_val == "PRODUCTION_MODE") {
        return(TRUE)
      }
    }
    
    # Default to false in development environment
    return(FALSE)
  }
  
  # Immediately stop if in production environment
  if (is_production()) {
    stop("ERROR: create_sample_amazon_data() cannot be used in production environments. ",
         "This function violates MP29 (No Fake Data) and is only for development/testing.")
  }
  
  # Load required packages
  library(dplyr)
  library(tibble)
  library(purrr)
  
  # Input validation
  if (!inherits(target_db, "DBIConnection")) {
    stop("target_db must be a DBI database connection")
  }
  
  # Display warning about test-only usage
  if (verbose) {
    message("WARNING: This function creates synthetic test data and should NEVER be used in production!")
    message("Generating sample Amazon sales data for DEVELOPMENT/TESTING only...")
  }
  
  # Generate random customer IDs
  customer_ids <- paste0("customer", sprintf("%03d", 1:num_customers))
  
  # Generate a sample list of products
  products <- tibble(
    sku = paste0("SKU", sprintf("%04d", 1:20)),
    product_name = paste("Product", LETTERS[1:20]),
    base_price = runif(20, min = 10, max = 100)
  )
  
  # Generate purchase data for each customer
  purchase_data <- map_dfr(customer_ids, function(cid) {
    # Determine number of purchases for this customer
    num_purchases <- sample(min_purchases:max_purchases, 1)
    
    # Generate purchase dates
    purchase_dates <- sort(sample(
      seq(as.Date(start_date), as.Date(end_date), by = "day"), 
      num_purchases, replace = TRUE))
    
    # Create purchase records
    map_dfr(1:num_purchases, function(i) {
      # Select a random product
      prod_idx <- sample(1:nrow(products), 1)
      selected_product <- products[prod_idx, ]
      
      # Generate price with some variation
      price <- selected_product$base_price * runif(1, min = 0.9, max = 1.1)
      
      # Generate quantity
      qty <- sample(1:3, 1)
      
      # Create record
      tibble(
        customer_id = cid,
        time = as.POSIXct(purchase_dates[i]),
        sku = selected_product$sku,
        lineproduct_price = round(price, 2),
        order_id = paste0("ORDER", sprintf("%06d", sample(1:999999, 1))),
        quantity = qty
      )
    })
  })
  
  # Add a clear marker that this is fake/sample data
  purchase_data <- purchase_data %>%
    mutate(
      is_sample_data = TRUE,
      sample_created_at = Sys.time(),
      sample_created_by = Sys.info()[["user"]]
    )
  
  # Write to database
  if (verbose) message("Writing ", nrow(purchase_data), " sample records to database...")
  
  # Create a table name with clear indication this is sample data
  table_name <- "amazon_sales_sample_dta"
  
  DBI::dbWriteTable(target_db, table_name, purchase_data, 
                   overwrite = overwrite)
  
  if (verbose) {
    message("Sample data created successfully in table: ", table_name)
    message("  - Customers: ", length(unique(purchase_data$customer_id)))
    message("  - Orders: ", length(unique(purchase_data$order_id)))
    message("  - Line products: ", nrow(purchase_data))
    message("  - Date range: ", min(purchase_data$time), " to ", max(purchase_data$time))
    message("NOTE: This data contains the 'is_sample_data' column to clearly mark it as non-production data")
  }
  
  # Return connection for chaining
  invisible(target_db)
}