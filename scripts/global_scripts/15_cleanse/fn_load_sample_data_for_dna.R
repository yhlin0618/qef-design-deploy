#' Load Sample Data for DNA Testing
#'
#' This helper function creates sample data in both raw and cleansed databases
#' to support testing of the customer DNA analysis pipeline.
#'
#' IMPORTANT: This function is for DEVELOPMENT and TESTING environments ONLY.
#' It should NEVER be used in production environments, as it violates MP29 (No Fake Data).
#'
#' @param force Logical. Whether to force overwrite existing tables. Default is FALSE.
#' @param verbose Logical. Whether to display detailed progress. Default is TRUE.
#' @param raw_conn DBI connection. Optional connection to raw_data database. If NULL, will create one.
#' @param cleansed_conn DBI connection. Optional connection to cleansed_data database. If NULL, will create one.
#'
#' @return No return value, called for side effects.
#'
#' @details
#' This function:
#' 1. Connects to both raw_data and cleansed_data databases
#' 2. Creates sample Amazon sales data in both databases
#' 3. Ensures the data has appropriate structure for DNA analysis
#' 4. Clearly marks all generated data as sample/test data
#'
#' @export
load_sample_data_for_dna <- function(force = FALSE, verbose = TRUE, 
                                     raw_conn = NULL, cleansed_conn = NULL) {
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
    stop("ERROR: load_sample_data_for_dna() cannot be used in production environments. ",
         "This function violates MP29 (No Fake Data) and is only for development/testing.")
  }
  
  # Display warning about test-only usage
  if (verbose) {
    message("WARNING: This function creates synthetic test data and should NEVER be used in production!")
    message("Loading sample data for DNA testing in DEVELOPMENT/TESTING environment only...")
  }
  
  library(dplyr)
  
  # Track if we need to clean up connections
  created_connections <- FALSE
  
  # Connect to databases if connections not provided
  if (is.null(raw_conn)) {
    if (!exists("fn_dbConnect_from_list") && !exists("dbConnect_from_list")) {
      stop("Database connection function not found. Please source database utility functions first.")
    }
    
    # Use the connection function that exists
    db_connect_fn <- if(exists("fn_dbConnect_from_list")) fn_dbConnect_from_list else dbConnect_from_list
    
    raw_conn <- db_connect_fn("raw_data", read_only = FALSE)
    created_connections <- TRUE
  }
  
  if (is.null(cleansed_conn)) {
    if (!exists("fn_dbConnect_from_list") && !exists("dbConnect_from_list")) {
      stop("Database connection function not found. Please source database utility functions first.")
    }
    
    # Use the connection function that exists
    db_connect_fn <- if(exists("fn_dbConnect_from_list")) fn_dbConnect_from_list else dbConnect_from_list
    
    cleansed_conn <- db_connect_fn("cleansed_data", read_only = FALSE)
    created_connections <- TRUE
  }
  
  # Ensure create_sample_amazon_data function is available
  if (!exists("create_sample_amazon_data")) {
    source(file.path("update_scripts", "global_scripts", "15_cleanse", "fn_create_sample_data.R"))
    if (!exists("create_sample_amazon_data")) {
      stop("create_sample_amazon_data function not found. Please source fn_create_sample_data.R first.")
    }
  }
  
  # Check if tables already exist
  sample_table_name <- "amazon_sales_sample_dta"
  raw_exists <- DBI::dbExistsTable(raw_conn, sample_table_name)
  cleansed_exists <- DBI::dbExistsTable(cleansed_conn, sample_table_name)
  
  # If tables exist and force is FALSE, warn and exit
  if ((raw_exists || cleansed_exists) && !force) {
    message("Sample data tables already exist. Use force=TRUE to overwrite.")
    message("Raw sample data exists: ", raw_exists)
    message("Cleansed sample data exists: ", cleansed_exists)
    
    # Disconnect only if we created the connections
    if (created_connections) {
      if (exists("fn_dbDisconnect_all")) {
        fn_dbDisconnect_all()
      } else if (exists("dbDisconnect_all")) {
        dbDisconnect_all()
      } else {
        if (!is.null(raw_conn)) DBI::dbDisconnect(raw_conn)
        if (!is.null(cleansed_conn)) DBI::dbDisconnect(cleansed_conn)
      }
    }
    
    return(invisible(NULL))
  }
  
  # Create sample data in raw_data
  if (verbose) message("\nCreating sample raw data...")
  create_sample_amazon_data(
    raw_conn, 
    num_customers = 100,
    min_purchases = 1,
    max_purchases = 10,
    overwrite = TRUE,
    verbose = verbose
  )
  
  # Create more structured sample in cleansed_data
  if (verbose) message("\nCreating sample cleansed data...")
  create_sample_amazon_data(
    cleansed_conn, 
    num_customers = 100,
    min_purchases = 1,
    max_purchases = 10,
    overwrite = TRUE,
    verbose = verbose
  )
  
  # Disconnect only if we created the connections
  if (created_connections) {
    if (verbose) message("Closing database connections...")
    
    if (exists("fn_dbDisconnect_all")) {
      fn_dbDisconnect_all()
    } else if (exists("dbDisconnect_all")) {
      dbDisconnect_all()
    } else {
      if (!is.null(raw_conn)) DBI::dbDisconnect(raw_conn)
      if (!is.null(cleansed_conn)) DBI::dbDisconnect(cleansed_conn)
    }
  }
  
  if (verbose) message("\nSample data loaded successfully for DNA analysis testing!")
}