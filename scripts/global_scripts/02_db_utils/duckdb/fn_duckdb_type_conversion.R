# ============================================================================
# DuckDB Type Conversion Functions
# ============================================================================
# Purpose: Type conversion utilities for R to DuckDB and DuckDB to R
# Source: Extracted from DU03_data_types.qmd
# Reference: docs/en/part2_implementations/CH17_database_specifications/duckdb/DU03_data_types.qmd
# Created: 2025-08-28
# ============================================================================

library(dplyr)
library(purrr)
library(jsonlite)

# ============================================================================
# Section 1: Numeric Type Selection
# ============================================================================

#' Choose appropriate numeric type based on data
#' 
#' @param values Numeric vector
#' @return Character string specifying DuckDB type
#' @export
choose_numeric_type <- function(values) {
  if (all(values %% 1 == 0, na.rm = TRUE)) {
    max_val <- max(abs(values), na.rm = TRUE)
    if (max_val <= 127) return("TINYINT")
    if (max_val <= 32767) return("SMALLINT")
    if (max_val <= 2147483647) return("INTEGER")
    return("BIGINT")
  } else {
    # For decimal values
    if (all(round(values, 2) == values, na.rm = TRUE)) {
      return("DECIMAL(10,2)")  # Common for money
    }
    return("DOUBLE")
  }
}

# ============================================================================
# Section 2: String Preparation
# ============================================================================

#' Prepare strings for DuckDB storage
#' 
#' @param df Data frame
#' @return Data frame with cleaned string columns
#' @export
prepare_strings_for_duckdb <- function(df) {
  df %>%
    mutate(across(where(is.character), ~ {
      # Remove null bytes
      cleaned <- gsub("\\x00", "", .x)
      # Normalize encoding
      cleaned <- iconv(cleaned, from = "UTF-8", to = "UTF-8", sub = "")
      # Trim whitespace
      cleaned <- trimws(cleaned)
      # Replace empty strings with NULL
      na_if(cleaned, "")
    }))
}

# ============================================================================
# Section 3: Temporal Type Handling
# ============================================================================

#' Handle temporal types for DuckDB
#' 
#' @param df Data frame
#' @return Data frame with properly formatted temporal columns
#' @export
handle_temporal_types <- function(df) {
  df %>%
    mutate(
      # Convert Date columns
      across(where(~ inherits(., "Date")), as.Date),
      
      # Convert POSIXct to UTC for consistency
      across(where(~ inherits(., "POSIXct")), 
             ~ lubridate::with_tz(., "UTC")),
      
      # Convert character dates
      across(matches("_date$"), 
             ~ as.Date(., format = "%Y-%m-%d")),
      
      # Convert character timestamps
      across(matches("_datetime$|_timestamp$"),
             ~ as.POSIXct(., format = "%Y-%m-%d %H:%M:%S", 
                         tz = "UTC"))
    )
}

# ============================================================================
# Section 4: Boolean Handling
# ============================================================================

#' Prepare boolean columns for DuckDB
#' 
#' @param df Data frame
#' @return Data frame with normalized boolean columns
#' @export
prepare_boolean_columns <- function(df) {
  df %>%
    mutate(across(where(is.logical), ~ {
      case_when(
        is.na(.) ~ NA,
        . ~ TRUE,
        TRUE ~ FALSE
      )
    }))
}

# ============================================================================
# Section 5: List Column Handling
# ============================================================================

#' Handle list columns for DuckDB storage
#' 
#' @param df Data frame
#' @param list_cols Character vector of list column names
#' @return Data frame with processed list columns
#' @export
handle_list_columns <- function(df, list_cols) {
  for (col in list_cols) {
    if (col %in% names(df)) {
      # Option 1: Convert to JSON string
      df[[paste0(col, "_json")]] <- sapply(df[[col]], 
        function(x) jsonlite::toJSON(x, auto_unbox = TRUE))
      
      # Option 2: Extract summary statistics
      df[[paste0(col, "_count")]] <- sapply(df[[col]], length)
      df[[paste0(col, "_first")]] <- sapply(df[[col]], 
        function(x) if(length(x) > 0) x[[1]] else NA)
    }
  }
  return(df)
}

# ============================================================================
# Section 6: Comprehensive Type Conversion
# ============================================================================

#' Convert R types to DuckDB types
#' 
#' @param df Data frame
#' @return Named list mapping column names to DuckDB types
#' @export
convert_r_to_duckdb <- function(df) {
  type_map <- list()
  
  for (col in names(df)) {
    r_class <- class(df[[col]])[1]
    
    type_map[[col]] <- switch(r_class,
      "integer" = "INTEGER",
      "numeric" = {
        if (all(df[[col]] == as.integer(df[[col]]), na.rm = TRUE)) {
          "INTEGER"
        } else if (max(nchar(sub("^[^.]*\\.", "", 
                                 as.character(df[[col]]))), 
                       na.rm = TRUE) <= 2) {
          "DECIMAL(10,2)"
        } else {
          "DOUBLE"
        }
      },
      "character" = "VARCHAR",
      "logical" = "BOOLEAN",
      "Date" = "DATE",
      "POSIXct" = "TIMESTAMP",
      "factor" = "VARCHAR",
      "list" = "JSON",
      "VARCHAR"  # Default
    )
  }
  
  return(type_map)
}

# ============================================================================
# Section 7: Table Creation with Types
# ============================================================================

#' Create DuckDB table from data frame with explicit types
#' 
#' @param con DuckDB connection
#' @param table_name Character string table name
#' @param df Data frame
#' @export
create_table_from_df <- function(con, table_name, df) {
  type_map <- convert_r_to_duckdb(df)
  
  # Prepare data
  df_prepared <- df %>%
    mutate(across(where(is.factor), as.character)) %>%
    mutate(across(where(is.list), 
                  ~ map_chr(., ~ jsonlite::toJSON(., auto_unbox = TRUE))))
  
  # Create table with explicit types
  col_definitions <- paste(
    names(type_map),
    unlist(type_map),
    collapse = ", "
  )
  
  create_query <- sprintf(
    "CREATE TABLE %s (%s)",
    table_name,
    col_definitions
  )
  
  DBI::dbExecute(con, create_query)
  DBI::dbAppendTable(con, table_name, df_prepared)
}

# ============================================================================
# Section 8: NULL Handling
# ============================================================================

#' Handle NULL values in DuckDB queries
#' 
#' @param con DuckDB connection
#' @param table_name Character string table name
#' @param column Character string column name
#' @return Query results with NULL handling
#' @export
handle_nulls <- function(con, table_name, column) {
  # NULL comparison requires IS NULL/IS NOT NULL
  query <- sprintf("
    SELECT * FROM %s
    WHERE %s IS NOT NULL
  ", table_name, column)
  
  DBI::dbGetQuery(con, query)
}

# ============================================================================
# Section 9: Overflow Protection
# ============================================================================

#' Check for potential overflow before insertion
#' 
#' @param df Data frame
#' @return List of potential overflow issues
#' @export
check_overflow <- function(df) {
  issues <- list()
  
  # Check integer columns
  int_cols <- names(df)[sapply(df, is.integer)]
  for (col in int_cols) {
    if (any(abs(df[[col]]) > 2147483647, na.rm = TRUE)) {
      issues[[col]] <- "Potential INTEGER overflow, use BIGINT"
    }
  }
  
  # Check numeric precision
  num_cols <- names(df)[sapply(df, is.numeric)]
  for (col in num_cols) {
    max_digits <- max(nchar(as.character(abs(df[[col]]))), na.rm = TRUE)
    if (max_digits > 15) {
      issues[[col]] <- "Potential precision loss in DOUBLE, consider DECIMAL"
    }
  }
  
  return(issues)
}

# ============================================================================
# Section 10: Factor Level Management
# ============================================================================

#' Save factor levels separately for DuckDB storage
#' 
#' @param con DuckDB connection
#' @param df Data frame
#' @param table_name Character string base table name
#' @return Data frame with factors converted to integers
#' @export
save_factor_levels <- function(con, df, table_name) {
  factor_cols <- names(df)[sapply(df, is.factor)]
  
  for (col in factor_cols) {
    levels_df <- data.frame(
      level_id = seq_along(levels(df[[col]])),
      level_value = levels(df[[col]])
    )
    
    DBI::dbWriteTable(con, paste0(table_name, "_", col, "_levels"), 
                 levels_df, overwrite = TRUE)
    
    # Convert factor to integer codes
    df[[col]] <- as.integer(df[[col]])
  }
  
  return(df)
}

# ============================================================================
# Section 11: Timezone Standardization
# ============================================================================

#' Standardize timezones to UTC
#' 
#' @param df Data frame
#' @return Data frame with standardized timezones
#' @export
standardize_timezones <- function(df) {
  df %>%
    mutate(across(where(~ inherits(., "POSIXct")), 
                  ~ lubridate::with_tz(., "UTC")))
}