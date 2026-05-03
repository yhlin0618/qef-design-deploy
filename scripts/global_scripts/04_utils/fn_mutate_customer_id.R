#' Mutate Customer IDs Based on Email
#'
#' This function processes a dataframe after collect() to create consistent customer IDs.
#' It finds email addresses, extracts unique identifiers, and converts them to integers.
#' The function also saves the lookup table to app_data.duckdb for future reference.
#'
#' @param df A dataframe that has been collected from a database query
#' @param email_col Character. The name of the column containing email addresses. Default: "email"
#' @param output_col Character. The name of the output column for the customer IDs. Default: "customer_id"
#' @param save_lookup Logical. Whether to save the lookup table to app_data.duckdb. Default: TRUE
#' @param lookup_table_name Character. The name of the lookup table in the database. Default: "customer_id_lookup"
#' @param app_data_conn DBI Connection. An existing connection to app_data.duckdb. If NULL, a new connection will be created. Default: NULL
#' @param extract_pattern Character. Regular expression pattern to extract the customer ID part from email. Default: extract everything before the @ symbol
#' @param lowercase Logical. Whether to convert emails to lowercase before processing. Default: TRUE
#' @param remove_special Logical. Whether to remove special characters from extracted IDs. Default: TRUE
#' @param hash_method Character. Hashing method to use ("md5", "sha1", "sha256", or "none"). Default: "none"
#' @param verbose Logical. Whether to display processing messages. Default: TRUE
#'
#' @return A dataframe with the new customer_id column added
#'
#' @examples
#' \dontrun{
#' # Basic usage with default parameters
#' df_with_ids <- mutate_customer_id(collected_df)
#'
#' # Custom usage with specific parameters
#' df_with_ids <- mutate_customer_id(
#'   collected_df,
#'   email_col = "customer_email",
#'   output_col = "global_customer_id",
#'   lookup_table_name = "customer_email_lookup"
#' )
#' 
#' # Without saving to database
#' df_with_ids <- mutate_customer_id(
#'   collected_df,
#'   save_lookup = FALSE
#' )
#' }
#'
#' @export
mutate_customer_id <- function(df,
                               email_col = "email",
                               output_col = "customer_id",
                               save_lookup = TRUE,
                               lookup_table_name = "customer_id_lookup",
                               app_data_conn = NULL,
                               extract_pattern = NULL,
                               lowercase = TRUE,
                               remove_special = TRUE,
                               hash_method = "none",
                               verbose = TRUE) {
  
  # Load required packages
  require(dplyr)
  require(stringr)
  require(DBI)
  require(duckdb)
  
  if (hash_method != "none") {
    require(digest)
  }
  
  # Input validation
  if (!is.data.frame(df)) {
    stop("Input must be a dataframe")
  }
  
  if (!email_col %in% names(df)) {
    stop(paste("Email column", email_col, "not found in dataframe"))
  }
  
  # Display processing info if verbose is TRUE
  if (verbose) {
    message(paste("Processing", nrow(df), "rows with email column:", email_col))
  }
  
  # Define the email extraction pattern if not provided
  if (is.null(extract_pattern)) {
    extract_pattern <- "^([^@]+)@"  # Extract everything before the @ symbol
    if (verbose) {
      message("Using default extraction pattern to get text before @ symbol")
    }
  }
  
  if (verbose) {
    message("Step 1: Processing emails and creating customer IDs...")
  }
  
  # Process the dataframe
  result_df <- df %>%
    mutate(
      # Step 1: Convert emails to lowercase if requested
      email_temp = if (lowercase) tolower(!!sym(email_col)) else !!sym(email_col),
      
      # Step 2: Extract the identifier part from email
      id_part = str_extract(email_temp, extract_pattern),
      
      # Remove the captured group markers if using regex with parentheses
      id_part = ifelse(!is.na(id_part), 
                       str_replace(id_part, extract_pattern, "\\1"), 
                       NA_character_),
      
      # Step 3: Remove special characters if requested
      id_part = if (remove_special) {
        str_replace_all(id_part, "[^[:alnum:]]", "")
      } else {
        id_part
      },
      
      # Step 4: Apply hashing if requested
      id_part = case_when(
        hash_method == "md5" ~ sapply(id_part, function(x) ifelse(is.na(x), NA, digest::digest(x, algo = "md5"))),
        hash_method == "sha1" ~ sapply(id_part, function(x) ifelse(is.na(x), NA, digest::digest(x, algo = "sha1"))),
        hash_method == "sha256" ~ sapply(id_part, function(x) ifelse(is.na(x), NA, digest::digest(x, algo = "sha256"))),
        TRUE ~ id_part  # No hashing
      )
    )
  
  if (verbose) {
    message("Step 2: Creating lookup table with unique identifiers...")
  }
  
  # Create a lookup table of unique IDs and assign integer IDs
  unique_ids <- result_df %>%
    select(id_part) %>%
    filter(!is.na(id_part)) %>%
    distinct() %>%
    arrange(id_part) %>%
    mutate(
      !!sym(output_col) := row_number(),
      original_email_part = id_part,  # Keep the original email part for reference
      created_at = Sys.time()  # Add timestamp
    )
  
  # Save lookup table to app_data.duckdb if requested
  if (save_lookup) {
    if (verbose) {
      message("Step 3: Saving lookup table to app_data.duckdb...")
    }
    
    # Create or get connection to app_data.duckdb
    close_connection <- FALSE
    if (is.null(app_data_conn)) {
      # Check if we should look for the standard database paths
      if (exists("get_default_db_paths")) {
        if (verbose) {
          message("Using get_default_db_paths() to locate database")
        }
        db_paths <- get_default_db_paths()
        app_data_path <- db_paths[["app_data"]]
      } else {
        # Fallback to a standard location within APP_DIR or ROOT_PATH if available
        if (exists("APP_DIR")) {
          base_dir <- APP_DIR
        } else if (exists("ROOT_PATH")) {
          base_dir <- ROOT_PATH
        } else {
          base_dir <- getwd()
        }
        app_data_path <- file.path(base_dir, "app_data", "app_data.duckdb")
        
        # Make sure the directory exists
        dir.create(dirname(app_data_path), showWarnings = FALSE, recursive = TRUE)
      }
      
      if (verbose) {
        message(paste("Connecting to database at:", app_data_path))
      }
      
      app_data_conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = app_data_path)
      close_connection <- TRUE
    }
    
    # Check if the table already exists
    table_exists <- DBI::dbExistsTable(app_data_conn, lookup_table_name)
    
    if (table_exists) {
      if (verbose) {
        message(paste("Lookup table", lookup_table_name, "already exists - updating with new entries"))
      }
      
      # Get existing lookup table
      existing_lookup <- DBI::dbReadTable(app_data_conn, lookup_table_name)
      
      # Find new entries that don't exist in the current lookup table
      if ("original_email_part" %in% names(existing_lookup)) {
        new_entries <- unique_ids %>%
          anti_join(existing_lookup, by = "original_email_part")
        
        if (nrow(new_entries) > 0) {
          # If there are new entries, adjust their IDs to continue from the max ID
          max_id <- max(existing_lookup[[output_col]], na.rm = TRUE)
          
          new_entries <- new_entries %>%
            mutate(!!sym(output_col) := row_number() + max_id)
          
          # Append new entries to the lookup table
          DBI::dbAppendTable(app_data_conn, lookup_table_name, new_entries)
          
          if (verbose) {
            message(paste("Added", nrow(new_entries), "new entries to existing lookup table"))
          }
        } else {
          if (verbose) {
            message("No new entries to add to lookup table")
          }
        }
        
        # Use the combined lookup table (existing + new)
        unique_ids <- bind_rows(existing_lookup, new_entries)
      } else {
        # The existing table has a different structure - overwrite with warning
        warning(paste("Existing lookup table", lookup_table_name, "has incompatible structure - overwriting"))
        DBI::dbWriteTable(app_data_conn, lookup_table_name, unique_ids, overwrite = TRUE)
        
        if (verbose) {
          message(paste("Overwrote existing lookup table with", nrow(unique_ids), "entries"))
        }
      }
    } else {
      # Create new lookup table
      DBI::dbWriteTable(app_data_conn, lookup_table_name, unique_ids)
      
      if (verbose) {
        message(paste("Created new lookup table with", nrow(unique_ids), "entries"))
      }
    }
    
    # Close connection if we opened it
    if (close_connection && !is.null(app_data_conn)) {
      DBI::dbDisconnect(app_data_conn)
      if (verbose) {
        message("Database connection closed")
      }
    }
  }
  
  if (verbose) {
    message("Step 4: Joining customer IDs to original dataframe...")
  }
  
  # Join the integer IDs back to the original dataframe
  result_df <- result_df %>%
    left_join(unique_ids %>% select(id_part, !!sym(output_col)), by = "id_part") %>%
    select(-email_temp, -id_part)  # Remove temporary columns
  
  # Report results if verbose
  if (verbose) {
    n_unique <- nrow(unique_ids)
    n_matched <- sum(!is.na(result_df[[output_col]]))
    message("Results:")
    message(paste(" - Found", n_unique, "unique customer IDs"))
    message(paste(" - Successfully assigned IDs to", n_matched, "of", nrow(df), "rows"))
    if (n_matched < nrow(df)) {
      message(paste(" - Warning:", nrow(df) - n_matched, "rows have NA customer IDs"))
    }
    message("Customer ID generation complete")
  }
  
  return(result_df)
}