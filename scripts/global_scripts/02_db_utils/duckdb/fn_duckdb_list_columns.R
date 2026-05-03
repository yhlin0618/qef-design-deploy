# ============================================================================
# DuckDB-Specific List Column and JSON Handling Functions
# ============================================================================
# Purpose: DuckDB-specific strategies for handling LIST, JSON and nested types
# These functions use DuckDB's native JSON functions and SQL extensions
# Source: Extracted from DU08_data_type_handling.qmd and DU09_list_column_strategies.qmd
# Reference: docs/en/part2_implementations/CH17_database_specifications/duckdb/
# Created: 2025-08-28
# ============================================================================

library(DBI)
library(dplyr)
library(purrr)
library(tidyr)
library(jsonlite)
library(glue)
library(httr)

# ============================================================================
# Section 1: DuckDB-Specific List Column Storage
# ============================================================================

#' Normalize list column to separate table in DuckDB
#' 
#' @param df Data frame
#' @param id_col ID column name
#' @param list_col List column name
#' @param con DuckDB connection
#' @return List with main and list tables
#' @export
normalize_list_to_duckdb <- function(df, id_col, list_col, con) {
  # Create main table without list column
  main_table <- df %>%
    select(-all_of(list_col))
  
  # Create normalized table for list values
  list_table <- df %>%
    select(all_of(c(id_col, list_col))) %>%
    tidyr::unnest(cols = all_of(list_col)) %>%
    rename(value = all_of(list_col)) %>%
    mutate(
      sequence = row_number(),
      .by = all_of(id_col)
    )
  
  # Write to DuckDB
  DBI::dbWriteTable(con, paste0("main_", list_col), main_table, overwrite = TRUE)
  DBI::dbWriteTable(con, paste0("list_", list_col), list_table, overwrite = TRUE)
  
  # Create view for easy querying
  view_query <- glue::glue("
    CREATE OR REPLACE VIEW v_{list_col}_joined AS
    SELECT 
      m.*,
      LIST(l.value ORDER BY l.sequence) AS {list_col}
    FROM main_{list_col} m
    LEFT JOIN list_{list_col} l ON m.{id_col} = l.{id_col}
    GROUP BY m.{id_col}
  ")
  
  DBI::dbExecute(con, view_query)
  
  return(list(
    main = main_table,
    list = list_table
  ))
}

# ============================================================================
# Section 2: DuckDB JSON Functions
# ============================================================================

#' Handle JSON data using DuckDB's JSON functions
#' 
#' @param con DuckDB connection
#' @param table_name Table name
#' @param json_col Name of JSON column (default: "data")
#' @return Data frame with parsed JSON using DuckDB functions
#' @export
handle_json_data_duckdb <- function(con, table_name, json_col = "data") {
  # Use DuckDB's JSON extraction functions
  query <- glue::glue("
    SELECT 
      *,
      json_extract({json_col}, '$.name') AS extracted_name,
      json_extract({json_col}, '$.values') AS extracted_values,
      json_array_length(json_extract({json_col}, '$.items')) AS item_count,
      json_type({json_col}) AS json_data_type
    FROM {table_name}
  ")
  
  df <- DBI::dbGetQuery(con, query)
  
  # Additional processing can be done in R if needed
  df %>%
    mutate(
      # Parse JSON string to R object if needed
      values_parsed = map(extracted_values, ~jsonlite::fromJSON(. %||% "[]")),
      
      # Extract nested data
      first_value = map_dbl(values_parsed, 
                           ~ifelse(length(.) > 0, .[[1]], NA))
    )
}

#' Store data with JSON columns and create DuckDB view
#' 
#' @param df Data frame
#' @param table_name Table name
#' @param con DuckDB connection
#' @param json_cols Vector of column names that are JSON
#' @export
store_with_json_duckdb <- function(df, table_name, con, json_cols = NULL) {
  # Auto-detect list columns if not specified
  if (is.null(json_cols)) {
    list_cols <- names(df)[sapply(df, is.list)]
    
    # Convert lists to JSON
    for (col in list_cols) {
      df[[col]] <- sapply(
        df[[col]], 
        function(x) jsonlite::toJSON(x, auto_unbox = TRUE, null = "null"),
        USE.NAMES = FALSE
      )
    }
    json_cols <- list_cols
  }
  
  # Write to database
  DBI::dbWriteTable(con, table_name, df, overwrite = TRUE)
  
  # Create view with JSON extraction for each JSON column
  if (length(json_cols) > 0) {
    view_components <- c()
    
    for (col in json_cols) {
      view_components <- c(view_components,
        glue::glue("
          json_extract({col}, '$') AS {col}_parsed,
          json_array_length({col}) AS {col}_length,
          json_type({col}) AS {col}_type"
        ))
    }
    
    view_query <- glue::glue("
      CREATE OR REPLACE VIEW v_{table_name}_json AS
      SELECT 
        *,
        {paste(view_components, collapse = ',\n        ')}
      FROM {table_name}
    ")
    
    DBI::dbExecute(con, view_query)
  }
}

# ============================================================================
# Section 3: DuckDB Type Mapping and Table Creation
# ============================================================================

#' Map R types to DuckDB types
#' 
#' @param r_type R type name
#' @return Corresponding DuckDB type
#' @export
get_duckdb_type <- function(r_type) {
  type_map <- list(
    "integer" = "INTEGER",
    "numeric" = "DOUBLE",
    "character" = "VARCHAR",
    "logical" = "BOOLEAN",
    "Date" = "DATE",
    "POSIXct" = "TIMESTAMP",
    "list" = "JSON",
    "data.frame" = "JSON",
    "factor" = "VARCHAR"
  )
  
  return(type_map[[r_type]] %||% "VARCHAR")
}

#' Create DuckDB table with explicit types
#' 
#' @param con DuckDB connection
#' @param table_name Name for new table
#' @param df Data frame to use as template
#' @param primary_key Optional primary key column
#' @return NULL (creates table as side effect)
#' @export
create_typed_table_duckdb <- function(con, table_name, df, primary_key = NULL) {
  # Generate column definitions
  col_defs <- map2_chr(
    names(df),
    map_chr(df, ~class(.)[1]),
    ~paste(.x, get_duckdb_type(.y))
  )
  
  # Add primary key if specified
  if (!is.null(primary_key) && primary_key %in% names(df)) {
    col_defs[names(df) == primary_key] <- paste(
      col_defs[names(df) == primary_key], 
      "PRIMARY KEY"
    )
  }
  
  # Create table
  create_query <- glue::glue("
    CREATE TABLE IF NOT EXISTS {table_name} (
      {paste(col_defs, collapse = ',\n  ')}
    )
  ")
  
  DBI::dbExecute(con, create_query)
}

#' Detect JSON types from file using DuckDB's automatic detection
#' 
#' @param con DuckDB connection
#' @param json_file Path to JSON file
#' @return Data frame with detected types
#' @export
detect_json_types_duckdb <- function(con, json_file) {
  # Use DuckDB's automatic JSON type detection
  df_auto <- DBI::dbGetQuery(con, sprintf("SELECT * FROM '%s' LIMIT 1000", json_file))
  
  # Get column types
  types <- sapply(df_auto, class)
  
  # Get DuckDB's view of the types
  type_query <- glue::glue("
    SELECT 
      column_name,
      data_type
    FROM (
      SELECT * FROM '%s' LIMIT 0
    ) t
    JOIN information_schema.columns
    ON table_name = 't'
  ", json_file)
  
  duckdb_types <- tryCatch(
    DBI::dbGetQuery(con, type_query),
    error = function(e) NULL
  )
  
  result <- data.frame(
    column = names(types),
    r_type = as.character(types),
    duckdb_type = sapply(types, get_duckdb_type),
    stringsAsFactors = FALSE
  )
  
  if (!is.null(duckdb_types)) {
    result$detected_duckdb_type <- duckdb_types$data_type[match(result$column, duckdb_types$column_name)]
  }
  
  return(result)
}

# ============================================================================
# Section 4: DuckDB-Specific ETL Pipeline
# ============================================================================

#' Process API response with DuckDB-specific optimizations
#' 
#' @param api_endpoint API endpoint URL
#' @param con DuckDB connection
#' @param table_name Target table name
#' @return Processed data frame
#' @export
process_api_to_duckdb <- function(api_endpoint, con, table_name = "api_data") {
  # Step 1: Fetch data
  response <- httr::GET(api_endpoint) %>%
    httr::content(as = "text") %>%
    jsonlite::fromJSON(flatten = TRUE)
  
  # Step 2: Identify list columns
  list_cols <- names(response)[sapply(response, is.list)]
  cat("Found list columns:", paste(list_cols, collapse = ", "), "\n")
  
  # Step 3: Apply appropriate strategy for each column
  processed_df <- response
  json_columns <- c()
  
  for (col in list_cols) {
    col_data <- processed_df[[col]]
    
    # Analyze structure
    is_simple <- all(sapply(col_data, length) <= 1)
    is_uniform <- length(unique(sapply(col_data, class))) == 1
    avg_length <- mean(sapply(col_data, length))
    
    if (is_simple) {
      # Strategy 1: Direct extraction
      cat(col, "-> Direct extraction\n")
      processed_df[[col]] <- sapply(
        col_data, 
        function(x) if(length(x) > 0) x[[1]] else NA
      )
      
    } else {
      # Store as JSON for DuckDB to handle
      cat(col, "-> JSON storage for DuckDB\n")
      processed_df[[col]] <- sapply(
        col_data,
        function(x) jsonlite::toJSON(x, auto_unbox = TRUE),
        USE.NAMES = FALSE
      )
      json_columns <- c(json_columns, col)
    }
  }
  
  # Step 4: Write to DuckDB
  DBI::dbWriteTable(con, table_name, processed_df, overwrite = TRUE)
  
  # Step 5: Create indexes for JSON columns using DuckDB syntax
  for (col in json_columns) {
    index_query <- glue::glue("
      CREATE INDEX IF NOT EXISTS idx_{table_name}_{col}_length 
      ON {table_name}((json_array_length({col})))
    ")
    try(DBI::dbExecute(con, index_query), silent = TRUE)
  }
  
  # Step 6: Create a view with extracted JSON fields
  if (length(json_columns) > 0) {
    view_components <- paste0(
      "json_extract(", json_columns, ", '$') AS ", json_columns, "_data",
      collapse = ",\n      "
    )
    
    view_query <- glue::glue("
      CREATE OR REPLACE VIEW v_{table_name}_expanded AS
      SELECT 
        *,
        {view_components}
      FROM {table_name}
    ")
    
    DBI::dbExecute(con, view_query)
    cat("Created view: v_", table_name, "_expanded\n", sep = "")
  }
  
  return(processed_df)
}

# ============================================================================
# Section 5: DuckDB-Specific Validation
# ============================================================================

#' Validate list column processing for DuckDB storage
#' 
#' @param original_df Original data frame
#' @param processed_df Processed data frame
#' @param con DuckDB connection
#' @return Named list of test results
#' @export
validate_duckdb_storage <- function(original_df, processed_df, con) {
  tests <- list()
  
  # Test 1: No data loss
  tests$row_count <- nrow(processed_df) >= nrow(original_df)
  
  # Test 2: No list columns remain (unless stored as JSON)
  remaining_lists <- sapply(processed_df, is.list)
  tests$no_lists <- !any(remaining_lists)
  
  # Test 3: JSON columns are valid
  json_cols <- names(processed_df)[grepl("_json$", names(processed_df)) | 
                                   sapply(processed_df, function(x) {
                                     is.character(x) && all(sapply(head(x, 10), jsonlite::validate))
                                   })]
  
  if (length(json_cols) > 0) {
    tests$valid_json <- all(sapply(json_cols, function(col) {
      all(sapply(processed_df[[col]], jsonlite::validate))
    }))
  } else {
    tests$valid_json <- TRUE
  }
  
  # Test 4: Can write to DuckDB
  test_table <- paste0("test_", format(Sys.time(), "%Y%m%d_%H%M%S"))
  tests$writable <- tryCatch({
    DBI::dbWriteTable(con, test_table, processed_df, overwrite = TRUE)
    
    # Test that we can query it
    result <- DBI::dbGetQuery(con, sprintf("SELECT COUNT(*) as n FROM %s", test_table))
    
    # Clean up
    DBI::dbRemoveTable(con, test_table)
    
    result$n == nrow(processed_df)
  }, error = function(e) FALSE)
  
  # Test 5: DuckDB can parse JSON columns
  if (length(json_cols) > 0 && tests$writable) {
    test_table2 <- paste0("test2_", format(Sys.time(), "%Y%m%d_%H%M%S"))
    DBI::dbWriteTable(con, test_table2, processed_df, overwrite = TRUE)
    
    tests$json_queryable <- tryCatch({
      for (col in json_cols) {
        query <- sprintf("SELECT json_type(%s) FROM %s LIMIT 1", col, test_table2)
        DBI::dbGetQuery(con, query)
      }
      DBI::dbRemoveTable(con, test_table2)
      TRUE
    }, error = function(e) {
      try(DBI::dbRemoveTable(con, test_table2), silent = TRUE)
      FALSE
    })
  } else {
    tests$json_queryable <- NA
  }
  
  return(tests)
}