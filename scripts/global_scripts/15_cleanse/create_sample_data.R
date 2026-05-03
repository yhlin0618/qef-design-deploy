#' Create Sample Amazon Sales Data
#'
#' This function creates sample Amazon sales data for testing DNA analysis.
#' It generates synthetic data that mimics the structure of Amazon sales data
#' and loads it into the specified database.
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
  # Load required packages
  library(dplyr)
  library(tibble)
  library(purrr)
  
  # Input validation
  if (!inherits(target_db, "DBIConnection")) {
    stop("target_db must be a DBI database connection")
  }
  
  if (verbose) message("Generating sample Amazon sales data...")
  
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
  
  # Write to database
  if (verbose) message("Writing ", nrow(purchase_data), " sample records to database...")
  DBI::dbWriteTable(target_db, "amazon_sales_dta", purchase_data, 
                    overwrite = overwrite)
  
  if (verbose) {
    message("Sample data created successfully:")
    message("  - Customers: ", length(unique(purchase_data$customer_id)))
    message("  - Orders: ", length(unique(purchase_data$order_id)))
    message("  - Line products: ", nrow(purchase_data))
    message("  - Date range: ", min(purchase_data$time), " to ", max(purchase_data$time))
  }
  
  # Return connection for chaining
  invisible(target_db)
}

#' Load Sample Data for DNA Testing
#'
#' This helper function creates sample data in both raw and cleansed databases
#' to support testing of the customer DNA analysis pipeline.
#'
#' @param force Logical. Whether to force overwrite existing tables. Default is FALSE.
#' @param verbose Logical. Whether to display detailed progress. Default is TRUE.
#'
#' @return No return value, called for side effects.
#'
#' @details
#' This function:
#' 1. Connects to both raw_data and cleansed_data databases
#' 2. Creates sample Amazon sales data in both databases
#' 3. Ensures the data has appropriate structure for DNA analysis
#'
#' @export
load_sample_data_for_dna <- function(force = FALSE, verbose = TRUE) {
  library(dplyr)
  
  # Connect to databases
  raw_data <- dbConnect_from_list("raw_data", read_only = FALSE)
  cleansed_data <- dbConnect_from_list("cleansed_data", read_only = FALSE)
  
  # Check if tables already exist
  raw_exists <- DBI::dbExistsTable(raw_data, "amazon_sales_dta")
  cleansed_exists <- DBI::dbExistsTable(cleansed_data, "amazon_sales_dta")
  
  # If tables exist and force is FALSE, warn and exit
  if ((raw_exists || cleansed_exists) && !force) {
    message("Data tables already exist. Use force=TRUE to overwrite.")
    message("Raw data exists: ", raw_exists)
    message("Cleansed data exists: ", cleansed_exists)
    dbDisconnect_all()
    return(invisible(NULL))
  }
  
  # Create sample data in raw_data
  if (verbose) message("\nCreating sample raw data...")
  create_sample_amazon_data(
    raw_data, 
    num_customers = 100,
    min_purchases = 1,
    max_purchases = 10,
    overwrite = TRUE,
    verbose = verbose
  )
  
  # Create more structured sample in cleansed_data
  if (verbose) message("\nCreating sample cleansed data...")
  create_sample_amazon_data(
    cleansed_data, 
    num_customers = 100,
    min_purchases = 1,
    max_purchases = 10,
    overwrite = TRUE,
    verbose = verbose
  )
  
  # Disconnect from databases
  dbDisconnect_all()
  
  if (verbose) message("\nSample data loaded successfully for DNA analysis testing!")
}