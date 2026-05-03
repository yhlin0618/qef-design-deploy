# Database Setup Script
# This script initializes database connections for the five-layer data pipeline (DF00 Architecture)
# Add this to your initialization logic to ensure db_path_list includes all pipeline layers

# Function to initialize database paths and connections
initialize_database_connections <- function(verbose = TRUE) {
  # Basic check for DBI and duckdb packages
  if(!requireNamespace("DBI", quietly = TRUE)) install.packages("DBI")
  if(!requireNamespace("duckdb", quietly = TRUE)) install.packages("duckdb")
  
  # Load packages if not already loaded
  if (!exists("dbConnect", mode = "function")) library(DBI)
  if (!exists("duckdb", mode = "function")) library(duckdb)
  
  # Check if GLOBAL_DIR is available (after autoinit())
  if (exists("GLOBAL_DIR")) {
    db_utils_dir <- file.path(GLOBAL_DIR, "02_db_utils")
  } else {
    stop("GLOBAL_DIR is required in db_setup.R. Run autoinit() or set GLOBAL_DIR explicitly.")
  }
  
  # Source get_default_db_paths
  paths_function_path <- file.path(db_utils_dir, "duckdb", "fn_get_default_db_paths.R")
  if(file.exists(paths_function_path)) {
    source(paths_function_path)
    if(verbose) message("Sourced database paths function from: ", paths_function_path)
  } else {
    stop("Unable to find database paths function at: ", paths_function_path)
  }
  
  # Source dbConnect_from_list
  connect_function_path <- file.path(db_utils_dir, "fn_dbConnect_from_list.R")
  if(file.exists(connect_function_path)) {
    source(connect_function_path)
    if(verbose) message("Sourced database connection function from: ", connect_function_path)
  } else {
    stop("Unable to find database connection function at: ", connect_function_path)
  }
  
  # Initialize database paths if not already done
  if(!exists("db_path_list") && exists("get_default_db_paths")) {
    db_path_list <<- get_default_db_paths()
    if(verbose) {
      message("Five-layer data pipeline database paths initialized:")
      message("  Layer 1 (Raw): ", db_path_list$raw_data)
      message("  Layer 2 (Staged): ", db_path_list$staged_data)
      message("  Layer 3 (Transformed): ", db_path_list$transformed_data)
      message("  Layer 4 (Cleansed): ", db_path_list$cleansed_data)
      message("  Layer 5 (Processed): ", db_path_list$processed_data)
      message("  Layer 6 (Application): ", db_path_list$app_data)
      
      if(length(db_path_list) > 6) {
        message("Additional databases:")
        for(db_name in names(db_path_list)[-(1:6)]) {
          if(!db_name %in% c("raw_data", "staged_data", "transformed_data", "cleansed_data", "processed_data", "app_data")) {
            message("  - ", db_name, ": ", db_path_list[[db_name]])
          }
        }
      }
    }
  }
  
  # Create alias if not exists
  if(exists("fn_dbConnect_from_list") && !exists("dbConnect_from_list")) {
    dbConnect_from_list <<- fn_dbConnect_from_list
    if(verbose) message("Created alias: dbConnect_from_list -> fn_dbConnect_from_list")
  }
  
  # Return TRUE if successfully initialized
  return(exists("db_path_list") && exists("dbConnect_from_list"))
}

# Initialize database connections when this script is sourced
initialize_database_connections()

# Usage Example:
# source("update_scripts/global_scripts/db_setup.R")
# 
# # Five-layer data pipeline connections (DF00 Architecture)
# raw_conn <- dbConnect_from_list("raw_data")           # Layer 1: Original data
# staged_conn <- dbConnect_from_list("staged_data")     # Layer 2: File-preprocessed
# transformed_conn <- dbConnect_from_list("transformed_data") # Layer 3: Schema-standardized
# cleansed_conn <- dbConnect_from_list("cleansed_data") # Layer 4: Quality-assured
# processed_conn <- dbConnect_from_list("processed_data") # Layer 5: Business-processed
# app_conn <- dbConnect_from_list("app_data")           # Layer 6: Application-ready
