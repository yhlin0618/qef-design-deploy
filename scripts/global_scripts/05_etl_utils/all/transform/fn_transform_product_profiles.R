#' Transform product Profiles from Staged Data to Business-Ready Data
#'
#' This function transforms staged product profile data into standardized business format
#' with column mapping and optimized DuckDB schema. It follows the
#' 7-layer ETL architecture Phase 2 (Transform) specifications.
#'
#' @param staged_db_connection A DBI connection to the staged data database
#' @param transformed_db_connection A DBI connection to the transformed data database
#' @param table_pattern Optional regex pattern to filter staged tables 
#'                     (default: "^df_product_profile_.*___staged$")
#' @param overwrite_existing Logical, whether to overwrite existing transformed tables
#'                          (default: TRUE)
#'
#' @return A list containing transformation results and metadata
#' @note This function creates tables with names: df_product_profile_*___transformed
#'       Input tables should follow pattern: df_product_profile_*___staged
#'
#' @examples
#' \dontrun{
#' # Connect to databases
#' staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = TRUE)
#' transformed_data <- dbConnectDuckdb(db_path_list$transformed_data, read_only = FALSE)
#' 
#' # Transform all product profiles
#' transform_results <- transform_product_profiles(
#'   staged_db_connection = staged_data,
#'   transformed_db_connection = transformed_data
#' )
#' }
#'
#' @export
transform_product_profiles <- function(staged_db_connection,
                                   transformed_db_connection,
                                   table_pattern = "^df_product_profile_.*___staged$",
                                   overwrite_existing = TRUE) {
  
  # Validate parameters
  if (missing(staged_db_connection) || missing(transformed_db_connection)) {
    stop("Both staged_db_connection and transformed_db_connection are required")
  }
  
  # Get all staged product profile tables
  staged_tables <- DBI::dbListTables(staged_db_connection)
  product_profile_tables <- staged_tables[grepl(table_pattern, staged_tables)]
  
  if (length(product_profile_tables) == 0) {
    stop("No staged product profile tables found matching pattern: ", table_pattern)
  }
  
  message("TRANSFORM: Found ", length(product_profile_tables), 
          " staged product profile tables to transform")
  
  # Initialize results
  transform_results <- list(
    tables_processed = character(),
    total_rows_transformed = 0,
    processing_details = list()
  )
  
  # Process each staged product profile table
  for (table_name in product_profile_tables) {
    
    message("TRANSFORM: Processing ", table_name)
    
    tryCatch({
      
      # ------------------------------------------------------------------
      # 1. Load staged data
      # ------------------------------------------------------------------
      staged_data_df <- DBI::dbGetQuery(staged_db_connection, 
                                       paste("SELECT * FROM", table_name))
      
      if (nrow(staged_data_df) == 0) {
        message("TRANSFORM: Warning - ", table_name, " is empty, skipping")
        next
      }
      
      # Extract product line ID from table name
      product_line_match <- regmatches(table_name, 
                                     regexpr("df_product_profile_([^_]+)", table_name))
      product_line_id <- gsub("df_product_profile_", "", product_line_match)
      
      message("TRANSFORM: Detected product line: ", product_line_id)
      
      # ------------------------------------------------------------------
      # 2. Standardize basic fields (fixed order)
      # ------------------------------------------------------------------
      message("TRANSFORM: Pre-standardize - staged_data_df dimensions: ", 
              nrow(staged_data_df), " x ", ncol(staged_data_df))
      
      transformed_df <- standardize_basic_fields(staged_data_df, product_line_id)
      
      message("TRANSFORM: Post-standardize - transformed_df dimensions: ", 
              nrow(transformed_df), " x ", ncol(transformed_df))
      
      if (nrow(transformed_df) == 0) {
        stop("TRANSFORM: standardize_basic_fields returned 0 rows - this is the source of the DuckDB error")
      }
      
      # ------------------------------------------------------------------
      # 3. Add any remaining useful columns from staged data
      # ------------------------------------------------------------------
      # For now, we only need the basic standardized fields
      # Additional columns can be added here if needed
      
      message("TRANSFORM: Using standardized fields only - dimensions: ",
              nrow(transformed_df), " x ", ncol(transformed_df))
      
      # ------------------------------------------------------------------
      # 4. Add transformation metadata
      # ------------------------------------------------------------------
      transformed_df <- transformed_df %>%
        dplyr::mutate(
          etl_transform_timestamp = Sys.time(),
          etl_phase = "transform",
          schema_version = "2TR_v2.1_direct_mapping"
        )
      
      # ------------------------------------------------------------------
      # 5. Create optimized DuckDB table structure
      # ------------------------------------------------------------------
      transformed_table_name <- gsub("___staged$", "___transformed", table_name)
      
      # Drop existing table if overwrite is enabled
      if (overwrite_existing && DBI::dbExistsTable(transformed_db_connection, transformed_table_name)) {
        DBI::dbExecute(transformed_db_connection, paste("DROP TABLE", transformed_table_name))
        message("TRANSFORM: Dropped existing table ", transformed_table_name)
      }
      
      # ------------------------------------------------------------------
      # 5. 直接寫入資料 (dbWriteTable 會自動建表)
      # ------------------------------------------------------------------
      
      # Debug: Check data frame integrity before write
      message("TRANSFORM: Pre-write validation - transformed_df dimensions: ", 
              nrow(transformed_df), " x ", ncol(transformed_df))
      
      # Check for any problematic columns
      for (col_name in names(transformed_df)) {
        col_length <- length(transformed_df[[col_name]])
        if (col_length != nrow(transformed_df)) {
          stop("TRANSFORM: Column length error - '", col_name, "' has ", col_length, 
               " values but data frame has ", nrow(transformed_df), " rows")
        }
      }
      
      # Try the write operation with better error handling
      write_success <- FALSE
      write_error <- NULL
      
      tryCatch({
        # Use suppressWarnings to handle DuckDB replace messages
        suppressWarnings({
          DBI::dbWriteTable(
            conn      = transformed_db_connection,
            name      = transformed_table_name,
            value     = transformed_df,
            append    = FALSE,
            overwrite = TRUE,  # Changed to TRUE to avoid replace conflicts
            row.names = FALSE
          )
        })
        write_success <- TRUE
        message("TRANSFORM: Successfully wrote data to table ", transformed_table_name)
      }, error = function(e) {
        write_error <<- e
        message("TRANSFORM: Write error captured: ", e$message)
      })
      
      # Check if write actually succeeded by querying the table
      if (write_success) {
        tryCatch({
          count_check <- DBI::dbGetQuery(transformed_db_connection, 
                                       paste0("SELECT COUNT(*) as count FROM ", transformed_table_name))
          actual_count <- count_check$count
          if (actual_count > 0) {
            message("TRANSFORM: Verification successful - ", actual_count, " rows written")
          } else {
            message("TRANSFORM: Warning - table created but contains 0 rows")
          }
        }, error = function(e) {
          message("TRANSFORM: Could not verify write success: ", e$message)
        })
      } else {
        stop("TRANSFORM: Failed to write table ", transformed_table_name, 
             ": ", ifelse(is.null(write_error), "Unknown error", write_error$message))
      }
      
      # Get final row count for reporting
      count_query <- paste0("SELECT COUNT(*) as count FROM ", transformed_table_name)
      final_count <- DBI::dbGetQuery(transformed_db_connection, count_query)$count
      
      # Update results
      transform_results$tables_processed <- c(transform_results$tables_processed,
                                             transformed_table_name)
      transform_results$total_rows_transformed <- transform_results$total_rows_transformed + final_count
      transform_results$processing_details[[transformed_table_name]] <- list(
        source_table = table_name,
        product_line_id = product_line_id,
        rows_processed = final_count,
        columns_processed = ncol(transformed_df),
        basic_fields_only = TRUE,
        schema_optimized = TRUE
      )
      
      message("TRANSFORM: Successfully transformed ", final_count, 
              " rows to ", transformed_table_name)
      
    }, error = function(e) {
      error_msg <- paste("Error transforming", table_name, ":", e$message)
      warning(error_msg)
      transform_results$processing_details[[paste0(table_name, "_ERROR")]] <- 
        list(error = e$message)
    })
  }
  
  # Final summary
  message("TRANSFORM: Completed processing ", length(product_profile_tables), 
          " tables")
  message("TRANSFORM: Successfully transformed ", 
          length(transform_results$tables_processed), " tables")
  message("TRANSFORM: Total rows transformed: ", transform_results$total_rows_transformed)
  
  return(transform_results)
}

#' Standardize Basic Fields to Fixed Order Structure
#'
#' Converts basic product fields to standardized naming and order using alias configuration:
#' product_brand, product_id, product_title, product_line_id, price, rating, num_rating
#'
#' @param df Data frame with staged data
#' @param product_line_id Product line identifier
#' @return Data frame with standardized basic fields
#' @keywords internal
standardize_basic_fields <- function(df, product_line_id) {
  
  # Use column aliases from app_configs
  # YAML file column_aliases is available directly in app_configs
  if (exists("app_configs") && "column_aliases" %in% names(app_configs)) {
    reverse_lookup <- build_reverse_lookup(app_configs$column_aliases)
  } else {
    warning("Column aliases configuration not found, using fallback logic")
    reverse_lookup <- list()  # Empty lookup will fall back to direct matching
  }
  
  # Define standard field order and their data types
  standard_fields <- list(
    product_brand = list(type = "character", default = NA_character_),
    product_id = list(type = "character", default = NA_character_),
    product_title = list(type = "character", default = NA_character_),
    product_line_id = list(type = "character", default = product_line_id),
    price = list(type = "numeric", default = NA_real_),
    rating = list(type = "numeric", default = NA_real_),
    num_rating = list(type = "integer", default = NA_integer_)
  )
  
  n <- nrow(df)
  
  # Pre-allocate data frame with correct dimensions to avoid 0-row issues
  standardized_df <- data.frame(
    product_brand = rep(NA_character_, n),
    product_id = rep(NA_character_, n), 
    product_title = rep(NA_character_, n),
    product_line_id = rep(product_line_id, n),
    price = rep(NA_real_, n),
    rating = rep(NA_real_, n),
    num_rating = rep(NA_integer_, n),
    stringsAsFactors = FALSE
  )
  
  message("TRANSFORM: Pre-allocated standardized_df with ", nrow(standardized_df), " rows")

  # Process each standard field (excluding product_line_id which is already set)
  for (standard_name in names(standard_fields)) {
    
    if (standard_name == "product_line_id") {
      # Already set during pre-allocation
      message("TRANSFORM: product_line_id already set with ", length(standardized_df[[standard_name]]), " values")
      next
    }
    
    # Find matching column in source data
    source_col <- find_source_column(df, standard_name, reverse_lookup)
    
    if (!is.null(source_col)) {
      # Convert to appropriate data type
      field_config <- standard_fields[[standard_name]]
      
      tryCatch({
        if (field_config$type == "numeric") {
          converted_values <- as.numeric(df[[source_col]])
        } else if (field_config$type == "integer") {
          converted_values <- as.integer(df[[source_col]])
        } else {
          converted_values <- as.character(df[[source_col]])
        }
        
        # Verify length before assignment
        if (length(converted_values) != n) {
          warning("TRANSFORM: Length mismatch for '", standard_name, "': expected ", n, ", got ", length(converted_values))
          # Force to correct length
          if (length(converted_values) < n) {
            converted_values <- c(converted_values, rep(field_config$default, n - length(converted_values)))
          } else {
            converted_values <- converted_values[1:n]
          }
        }
        
        # Update the pre-allocated column
        standardized_df[[standard_name]] <- converted_values
        message("TRANSFORM: Mapped '", source_col, "' -> '", standard_name, "' (", length(converted_values), " values)")
        
      }, error = function(e) {
        warning("TRANSFORM: Error converting column '", source_col, "' for '", standard_name, "': ", e$message)
        # Keep the default NA values that were pre-allocated
        message("TRANSFORM: Using default values for '", standard_name, "' due to conversion error")
      })
      
    } else {
      # Keep default values that were pre-allocated
      message("TRANSFORM: No source found for '", standard_name, "', keeping default values")
    }
    
    # Verify each column still has correct length
    if (length(standardized_df[[standard_name]]) != n) {
      stop("TRANSFORM: Critical error - column '", standard_name, "' has length ", 
           length(standardized_df[[standard_name]]), " but expected ", n)
    }
  }
  
  # Collect used source columns for exclusion
  used_cols <- sapply(names(standard_fields), function(std_name) {
    if (std_name == "product_line_id") return(NULL)
    find_source_column(df, std_name, reverse_lookup)
  })
  used_cols <- used_cols[!sapply(used_cols, is.null)]
  
  # Add ETL metadata columns to exclude list
  exclude_cols <- c(unlist(used_cols), 
                   "etl_staging_timestamp", "etl_validation_status", "etl_phase")
  
  # Add remaining fields for further processing
  other_fields <- df[, !names(df) %in% exclude_cols, drop = FALSE]
  
  # Debug: Verify dimensions before cbind
  message("TRANSFORM: standardized_df dimensions: ", nrow(standardized_df), " x ", ncol(standardized_df))
  message("TRANSFORM: other_fields dimensions: ", nrow(other_fields), " x ", ncol(other_fields))
  
  # Verify row counts match before cbind
  if (nrow(standardized_df) != nrow(other_fields)) {
    stop("TRANSFORM: Row count mismatch - standardized_df: ", nrow(standardized_df), 
         ", other_fields: ", nrow(other_fields))
  }
  
  # Combine standardized fields with other fields
  result_df <- cbind(standardized_df, other_fields)
  
  message("TRANSFORM: Standardized basic fields for ", nrow(result_df), " products")
  
  return(result_df)
}



#' Build Transform Column Definitions for DuckDB
#'
#' Creates optimized column definitions for DuckDB table creation
#'
#' @param df Transformed data frame
#' @param product_line_id Product line identifier
#' @return List of column definitions for generate_create_table_query
#' @keywords internal
build_transform_column_definitions <- function(df, product_line_id) {
  
  column_defs <- list()
  
  # Fixed basic fields with optimized types and constraints
  basic_fields <- list(
    list(name = "product_brand", type = "VARCHAR(100)", not_null = FALSE),
    list(name = "product_id", type = "VARCHAR(20)", not_null = TRUE),
    list(name = "product_title", type = "TEXT", not_null = FALSE),
    list(name = "product_line_id", type = "VARCHAR(10)", not_null = TRUE,
         check = sprintf("product_line_id IN ('%s')", product_line_id)),
    list(name = "price", type = "DECIMAL(10,2)", not_null = FALSE,
         check = "price IS NULL OR price >= 0"),
    list(name = "rating", type = "DECIMAL(3,1)", not_null = FALSE,
         check = "rating IS NULL OR (rating >= 0 AND rating <= 5)"),
    list(name = "num_rating", type = "INTEGER", not_null = FALSE,
         check = "num_rating IS NULL OR num_rating >= 0")
  )
  
  column_defs <- c(column_defs, basic_fields)
  
  # Process other columns
  other_cols <- setdiff(names(df), c("product_brand", "product_id", "product_title", 
                                    "product_line_id", "price", "rating", "num_rating"))
  
  for (col_name in other_cols) {
    
    col_data <- df[[col_name]]
    
    # Determine appropriate DuckDB type
    if (is.character(col_data) || is.factor(col_data)) {
      
      max_length <- max(nchar(as.character(col_data)), na.rm = TRUE)
      
      if (max_length <= 50) {
        col_type <- "VARCHAR(100)"
      } else if (max_length <= 255) {
        col_type <- "VARCHAR(500)"
      } else {
        col_type <- "TEXT"
      }
      
    } else if (is.integer(col_data)) {
      col_type <- "INTEGER"
    } else if (is.numeric(col_data)) {
      col_type <- "DECIMAL(15,4)"
    } else if (is.logical(col_data)) {
      col_type <- "BOOLEAN"
    } else if (inherits(col_data, "POSIXt")) {
      col_type <- "TIMESTAMP"
    } else {
      col_type <- "TEXT"  # Default fallback
    }
    
    column_defs[[length(column_defs) + 1]] <- list(
      name = col_name,
      type = col_type,
      not_null = FALSE
    )
  }
  
  # Add ETL metadata columns
  metadata_fields <- list(
    list(name = "etl_transform_timestamp", type = "TIMESTAMP", not_null = TRUE),
    list(name = "etl_phase", type = "VARCHAR(20)", not_null = TRUE),
    list(name = "schema_version", type = "VARCHAR(50)", not_null = TRUE)
  )
  
  column_defs <- c(column_defs, metadata_fields)
  
  return(column_defs)
}

#' Build Reverse Lookup Table for Column Aliases
#'
#' Creates a lookup table from original column names to standard names
#' using the loaded alias configuration
#'
#' @param alias_config List containing standard_names configuration
#' @return Named list where names are original columns, values are standard names
#' @keywords internal
build_reverse_lookup <- function(alias_config) {
  
  lookup <- list()
  
  if (!is.null(alias_config$standard_names)) {
    for (standard_name in names(alias_config$standard_names)) {
      aliases <- alias_config$standard_names[[standard_name]]
      
      # Handle both list and vector formats
      if (is.list(aliases)) {
        aliases <- unlist(aliases)
      }
      
      for (alias in aliases) {
        lookup[[alias]] <- standard_name
      }
    }
  }
  
  return(lookup)
}

#' Find Source Column for Standard Field
#'
#' Searches for a matching source column name using the reverse lookup table
#'
#' @param df Data frame to search in
#' @param standard_name Standard field name to find source for
#' @param reverse_lookup Reverse lookup table (original -> standard)
#' @return Character string of source column name, or NULL if not found
#' @keywords internal
find_source_column <- function(df, standard_name, reverse_lookup) {
  
  # Get all possible aliases for this standard name
  possible_aliases <- names(reverse_lookup)[reverse_lookup == standard_name]
  
  # Find which aliases exist in the data frame
  existing_aliases <- intersect(possible_aliases, names(df))
  
  if (length(existing_aliases) > 0) {
    # Return the first match (priority by order in config)
    return(existing_aliases[1])
  }
  
  # Check if the standard name itself exists in the data
  if (standard_name %in% names(df)) {
    return(standard_name)
  }
  
  return(NULL)
}