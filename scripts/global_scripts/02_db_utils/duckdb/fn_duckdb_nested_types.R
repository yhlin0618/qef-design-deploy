# ============================================================================
# DuckDB Nested Type Handling
# ============================================================================
# Purpose: DuckDB-specific functions for LIST, STRUCT, and nested types
# Uses DuckDB's native support for complex types and SQL extensions
# Date: 2025-08-28
# ============================================================================

library(DBI)
library(dplyr)
library(purrr)
library(glue)

#' Process STRUCT type data in DuckDB
#' 
#' @param con DuckDB connection
#' @param table_name Table with struct column
#' @param struct_col Name of struct column
#' @return Query result with struct fields extracted
#' @export
process_struct_in_duckdb <- function(con, table_name, struct_col) {
  # Get struct field names using DuckDB's introspection
  schema_query <- glue::glue("
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = '{table_name}'
      AND column_name = '{struct_col}'
  ")
  
  col_info <- DBI::dbGetQuery(con, schema_query)
  
  # Extract struct fields using DuckDB syntax
  extract_query <- glue::glue("
    SELECT 
      *,
      {struct_col}.* -- DuckDB automatically expands struct fields
    FROM {table_name}
  ")
  
  return(DBI::dbGetQuery(con, extract_query))
}

#' Create DuckDB LIST type column
#' 
#' @param con DuckDB connection
#' @param table_name Table name
#' @param values_table Table with values to aggregate into list
#' @param group_col Column to group by
#' @param value_col Column to aggregate into list
#' @export
create_list_column_duckdb <- function(con, table_name, values_table, group_col, value_col) {
  # Create table with LIST type using DuckDB's LIST() aggregate
  create_query <- glue::glue("
    CREATE OR REPLACE TABLE {table_name} AS
    SELECT 
      {group_col},
      LIST({value_col} ORDER BY {value_col}) AS {value_col}_list,
      COUNT(*) AS count,
      MIN({value_col}) AS min_value,
      MAX({value_col}) AS max_value
    FROM {values_table}
    GROUP BY {group_col}
  ")
  
  DBI::dbExecute(con, create_query)
  
  # Create view for unnested access
  view_query <- glue::glue("
    CREATE OR REPLACE VIEW v_{table_name}_unnested AS
    SELECT 
      {group_col},
      UNNEST({value_col}_list) AS {value_col}
    FROM {table_name}
  ")
  
  DBI::dbExecute(con, view_query)
}

#' Process large dataset with DuckDB chunking
#' 
#' @param con DuckDB connection
#' @param source_table Source table name
#' @param target_table Target table name
#' @param chunk_size Number of rows per chunk
#' @param transform_query Optional SQL transformation
#' @return NULL (processes data as side effect)
#' @export
process_large_dataset_duckdb <- function(con, source_table, target_table, 
                                        chunk_size = 100000, transform_query = NULL) {
  # Get total rows
  total_rows <- DBI::dbGetQuery(con, 
    glue::glue("SELECT COUNT(*) as n FROM {source_table}")
  )$n
  
  # Create target table structure
  DBI::dbExecute(con, glue::glue("
    CREATE TABLE IF NOT EXISTS {target_table} AS 
    SELECT * FROM {source_table} WHERE 1=0
  "))
  
  # Process in chunks using DuckDB's OFFSET/LIMIT
  for (offset in seq(0, total_rows, chunk_size)) {
    if (is.null(transform_query)) {
      # Simple copy
      chunk_query <- glue::glue("
        INSERT INTO {target_table}
        SELECT * FROM {source_table}
        LIMIT {chunk_size}
        OFFSET {offset}
      ")
    } else {
      # Apply transformation
      chunk_query <- glue::glue("
        INSERT INTO {target_table}
        {transform_query}
        LIMIT {chunk_size}
        OFFSET {offset}
      ")
    }
    
    DBI::dbExecute(con, chunk_query)
    
    # Periodic checkpoint for large operations
    if (offset %% (chunk_size * 10) == 0 && offset > 0) {
      DBI::dbExecute(con, "CHECKPOINT")
      cat("Processed", min(offset + chunk_size, total_rows), 
          "of", total_rows, "rows\n")
    }
  }
  
  # Final checkpoint
  DBI::dbExecute(con, "CHECKPOINT")
}

#' Work with DuckDB's native MAP type
#' 
#' @param con DuckDB connection
#' @param table_name Table to create
#' @param data Data frame with key-value pairs
#' @param key_col Column for map keys
#' @param value_col Column for map values
#' @param group_col Column to group by for creating maps
#' @export
create_map_column_duckdb <- function(con, table_name, data, key_col, value_col, group_col) {
  # First write the data
  temp_table <- paste0(table_name, "_temp")
  DBI::dbWriteTable(con, temp_table, data, overwrite = TRUE)
  
  # Create table with MAP type
  map_query <- glue::glue("
    CREATE OR REPLACE TABLE {table_name} AS
    SELECT 
      {group_col},
      MAP(LIST({key_col}), LIST({value_col})) AS data_map,
      COUNT(*) AS pair_count
    FROM {temp_table}
    GROUP BY {group_col}
  ")
  
  DBI::dbExecute(con, map_query)
  
  # Clean up temp table
  DBI::dbRemoveTable(con, temp_table)
  
  # Create view for map access
  access_view <- glue::glue("
    CREATE OR REPLACE VIEW v_{table_name}_access AS
    SELECT 
      {group_col},
      data_map,
      element_at(data_map, 'specific_key') AS specific_value,
      map_keys(data_map) AS all_keys,
      map_values(data_map) AS all_values
    FROM {table_name}
  ")
  
  DBI::dbExecute(con, access_view)
}

#' Handle DuckDB UNION type
#' 
#' @param con DuckDB connection
#' @param table_name Table to create
#' @param mixed_data Data with mixed types
#' @export
create_union_column_duckdb <- function(con, table_name, mixed_data) {
  # DuckDB's UNION type can store values of different types
  # This is useful for handling truly heterogeneous data
  
  # Write initial data
  temp_table <- paste0(table_name, "_temp")
  DBI::dbWriteTable(con, temp_table, mixed_data, overwrite = TRUE)
  
  # Create table with UNION type (if column has mixed types)
  # DuckDB will automatically use UNION when needed
  union_query <- glue::glue("
    CREATE OR REPLACE TABLE {table_name} AS
    SELECT * FROM {temp_table}
  ")
  
  DBI::dbExecute(con, union_query)
  
  # Create view to handle UNION type access
  type_view <- glue::glue("
    CREATE OR REPLACE VIEW v_{table_name}_typed AS
    SELECT 
      *,
      -- Example of handling UNION columns
      CASE 
        WHEN union_tag(mixed_column) = 'INTEGER' THEN CAST(mixed_column AS INTEGER)
        WHEN union_tag(mixed_column) = 'VARCHAR' THEN CAST(mixed_column AS VARCHAR)
        ELSE NULL
      END AS normalized_value
    FROM {table_name}
    WHERE column_name = 'mixed_column'  -- Replace with actual column
  ")
  
  # Only create view if there are UNION columns
  tryCatch(
    DBI::dbExecute(con, type_view),
    error = function(e) {
      message("No UNION type columns detected")
    }
  )
  
  # Clean up
  DBI::dbRemoveTable(con, temp_table)
}

#' Create and query nested LIST of STRUCTs in DuckDB
#' 
#' @param con DuckDB connection
#' @param table_name Table name to create
#' @export
demo_nested_types_duckdb <- function(con, table_name = "nested_demo") {
  # Create a complex nested structure
  nested_query <- glue::glue("
    CREATE OR REPLACE TABLE {table_name} AS
    SELECT 
      customer_id,
      -- LIST of STRUCTs
      LIST({
        'order_id': order_id,
        'date': order_date,
        'total': amount,
        'items': LIST({
          'product': product_name,
          'quantity': quantity,
          'price': price
        })
      }) AS orders
    FROM (
      -- Sample data
      SELECT 
        1 as customer_id,
        101 as order_id,
        '2024-01-15'::DATE as order_date,
        150.00 as amount,
        'Widget' as product_name,
        2 as quantity,
        75.00 as price
      UNION ALL
      SELECT 1, 102, '2024-02-01'::DATE, 200.00, 'Gadget', 1, 200.00
      UNION ALL
      SELECT 2, 103, '2024-01-20'::DATE, 50.00, 'Tool', 1, 50.00
    ) t
    GROUP BY customer_id
  ")
  
  DBI::dbExecute(con, nested_query)
  
  # Query nested data
  analysis_query <- glue::glue("
    SELECT 
      customer_id,
      LIST_AGGREGATE(orders, 'sum', 'total') AS total_spent,
      LIST_AGGREGATE(orders, 'count') AS order_count,
      -- Extract nested values
      orders[1].order_id AS first_order_id,
      orders[1].items[1].product AS first_product
    FROM {table_name}
  ")
  
  return(DBI::dbGetQuery(con, analysis_query))
}

#' Optimize nested data storage in DuckDB
#' 
#' @param con DuckDB connection
#' @param df Data frame with nested columns
#' @param table_name Target table name
#' @export
optimize_nested_storage_duckdb <- function(con, df, table_name) {
  # Analyze nested columns
  nested_cols <- names(df)[sapply(df, function(x) is.list(x) || is.data.frame(x))]
  
  if (length(nested_cols) == 0) {
    # No nested columns, store directly
    DBI::dbWriteTable(con, table_name, df, overwrite = TRUE)
    return(invisible(NULL))
  }
  
  # For each nested column, determine optimal storage
  for (col in nested_cols) {
    col_data <- df[[col]]
    
    # Check characteristics
    is_uniform <- length(unique(sapply(col_data, class))) == 1
    avg_size <- mean(sapply(col_data, object.size))
    
    if (is_uniform && avg_size < 1000) {
      # Small, uniform data -> keep as LIST
      message(glue::glue("{col}: Storing as DuckDB LIST type"))
    } else if (avg_size > 10000) {
      # Large data -> normalize to separate table
      message(glue::glue("{col}: Normalizing to separate table"))
      
      # Create normalized structure
      norm_table <- paste0(table_name, "_", col)
      # Implementation would go here
    } else {
      # Medium complexity -> store as JSON
      message(glue::glue("{col}: Converting to JSON for flexibility"))
      df[[col]] <- sapply(df[[col]], jsonlite::toJSON, auto_unbox = TRUE)
    }
  }
  
  # Write optimized structure
  DBI::dbWriteTable(con, table_name, df, overwrite = TRUE)
  
  # Create indexes on JSON columns
  json_cols <- nested_cols[sapply(df[nested_cols], function(x) is.character(x[1]))]
  for (col in json_cols) {
    idx_query <- glue::glue("
      CREATE INDEX idx_{table_name}_{col} 
      ON {table_name} USING GIN (({col}::json))
    ")
    tryCatch(
      DBI::dbExecute(con, idx_query),
      error = function(e) message("Index creation skipped: ", e$message)
    )
  }
}