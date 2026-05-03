# ============================================================================
# DuckDB Import/Export Functions
# ============================================================================
# Purpose: Data import and export utilities for various file formats
# Source: Extracted from DU04_import_export.qmd
# Reference: docs/en/part2_implementations/CH17_database_specifications/duckdb/DU04_import_export.qmd
# Created: 2025-08-28
# ============================================================================

library(DBI)
library(duckdb)
library(glue)

# ============================================================================
# Section 1: CSV Import/Export
# ============================================================================

#' Import CSV with advanced options
#' 
#' @param con DuckDB connection
#' @param file_path Path to CSV file
#' @param table_name Target table name
#' @export
import_csv_advanced <- function(con, file_path, table_name) {
  query <- sprintf("
    CREATE TABLE %s AS 
    SELECT * FROM read_csv(
      '%s',
      -- Delimiter and quoting
      delim = ',',
      quote = '\"',
      escape = '\"',
      
      -- Header and column handling
      header = true,
      columns = 'auto',  -- or specify types
      auto_detect = true,
      sample_size = 20000,
      
      -- Null handling
      nullstr = ['NA', 'NULL', ''],
      
      -- Performance options
      parallel = true,
      buffer_size = 1000000,
      
      -- Data cleaning
      normalize_names = true,
      skip = 0,
      max_line_size = 1048576,
      
      -- Type detection
      all_varchar = false,
      dateformat = '%%Y-%%m-%%d',
      timestampformat = '%%Y-%%m-%%d %%H:%%M:%%S'
    )
  ", table_name, file_path)
  
  DBI::dbExecute(con, query)
}

#' Export data to CSV with options
#' 
#' @param con DuckDB connection
#' @param table_name Source table name
#' @param file_path Output CSV file path
#' @param options List of export options
#' @export
export_csv <- function(con, table_name, file_path, options = list()) {
  # Basic export
  if (length(options) == 0) {
    DBI::dbExecute(con, sprintf("
      COPY %s TO '%s' (FORMAT CSV, HEADER true)
    ", table_name, file_path))
    return(invisible(TRUE))
  }
  
  # Advanced export with options
  options_str <- paste(
    sprintf("FORMAT CSV"),
    sprintf("DELIMITER '%s'", options$delimiter %||% ","),
    sprintf("HEADER %s", tolower(as.character(options$header %||% TRUE))),
    sprintf("QUOTE '%s'", options$quote %||% '"'),
    sprintf("ESCAPE '%s'", options$escape %||% '"'),
    if (!is.null(options$force_quote)) 
      sprintf("FORCE_QUOTE (%s)", paste(options$force_quote, collapse = ",")) 
    else "",
    sep = ", "
  )
  
  DBI::dbExecute(con, sprintf("
    COPY (SELECT * FROM %s) TO '%s' (%s)
  ", table_name, file_path, options_str))
}

#' Import multiple CSV files using glob pattern
#' 
#' @param con DuckDB connection
#' @param pattern Glob pattern for file matching
#' @param table_name Target table name
#' @export
import_csv_glob <- function(con, pattern, table_name) {
  # Import all matching files
  DBI::dbExecute(con, sprintf("
    CREATE TABLE %s AS 
    SELECT * FROM read_csv_auto('%s')
  ", table_name, pattern))
}

#' Import CSV with source file tracking
#' 
#' @param con DuckDB connection
#' @param pattern Glob pattern for files
#' @param table_name Target table name
#' @export
import_with_source <- function(con, pattern, table_name) {
  DBI::dbExecute(con, sprintf("
    CREATE TABLE %s AS 
    SELECT *, filename FROM read_csv_auto('%s', filename=true)
  ", table_name, pattern))
}

# ============================================================================
# Section 2: Parquet Import/Export
# ============================================================================

#' Import Parquet files
#' 
#' @param con DuckDB connection
#' @param file_path Path to Parquet file(s)
#' @param table_name Target table name
#' @param columns Optional column selection
#' @export
import_parquet <- function(con, file_path, table_name, columns = NULL) {
  if (is.null(columns)) {
    # Basic import
    DBI::dbExecute(con, sprintf("
      CREATE TABLE %s AS 
      SELECT * FROM read_parquet('%s')
    ", table_name, file_path))
  } else {
    # Import with column selection
    col_list <- paste(columns, collapse = ", ")
    DBI::dbExecute(con, sprintf("
      CREATE TABLE %s AS 
      SELECT %s FROM read_parquet('%s')
    ", table_name, col_list, file_path))
  }
}

#' Query Parquet file directly without importing
#' 
#' @param con DuckDB connection
#' @param file_path Path to Parquet file
#' @param query SQL query to execute
#' @return Query results
#' @export
query_parquet <- function(con, file_path, query = NULL) {
  if (is.null(query)) {
    query <- sprintf("
      SELECT 
        COUNT(*) as row_count,
        COUNT(DISTINCT *) as unique_rows
      FROM read_parquet('%s')
    ", file_path)
  } else {
    query <- gsub("\\{file\\}", sprintf("read_parquet('%s')", file_path), query)
  }
  
  DBI::dbGetQuery(con, query)
}

#' Export data to Parquet format
#' 
#' @param con DuckDB connection
#' @param table_name Source table name
#' @param file_path Output file path
#' @param options Export options
#' @export
export_parquet <- function(con, table_name, file_path, options = list()) {
  # Basic export
  if (length(options) == 0) {
    DBI::dbExecute(con, sprintf("
      COPY %s TO '%s' (FORMAT PARQUET)
    ", table_name, file_path))
    return(invisible(TRUE))
  }
  
  # Export with compression and row group options
  DBI::dbExecute(con, sprintf("
    COPY %s TO '%s' (
      FORMAT PARQUET, 
      COMPRESSION '%s',
      ROW_GROUP_SIZE %d
    )
  ", table_name, file_path, 
     options$compression %||% "snappy",
     options$row_group_size %||% 122880))
}

#' Export partitioned Parquet files
#' 
#' @param con DuckDB connection
#' @param table_name Source table name
#' @param base_path Base output path
#' @param partition_cols Columns to partition by
#' @export
export_partitioned_parquet <- function(con, table_name, base_path, partition_cols) {
  DBI::dbExecute(con, sprintf("
    COPY (SELECT * FROM %s) 
    TO '%s' 
    (FORMAT PARQUET, PARTITION_BY (%s))
  ", table_name, base_path, paste(partition_cols, collapse = ", ")))
}

# ============================================================================
# Section 3: JSON Import/Export
# ============================================================================

#' Import JSON data
#' 
#' @param con DuckDB connection
#' @param file_path Path to JSON file
#' @param table_name Target table name
#' @param columns Optional column specification
#' @export
import_json <- function(con, file_path, table_name, columns = NULL) {
  # Install and load JSON extension
  tryCatch({
    DBI::dbExecute(con, "INSTALL json")
  }, error = function(e) {
    # Extension might already be installed
  })
  DBI::dbExecute(con, "LOAD json")
  
  if (is.null(columns)) {
    # Auto-detect schema
    DBI::dbExecute(con, sprintf("
      CREATE TABLE %s AS 
      SELECT * FROM read_json_auto('%s')
    ", table_name, file_path))
  } else {
    # With specific column types
    col_spec <- paste(names(columns), columns, sep = ": ", collapse = ", ")
    DBI::dbExecute(con, sprintf("
      CREATE TABLE %s AS 
      SELECT * FROM read_json(
        '%s',
        columns = {%s},
        format = 'auto',
        records = 'auto'
      )
    ", table_name, file_path, col_spec))
  }
}

#' Process nested JSON data
#' 
#' @param con DuckDB connection
#' @param file_path Path to JSON file
#' @param table_name Target table name
#' @param extraction_spec List of JSON path extractions
#' @export
process_nested_json <- function(con, file_path, table_name, extraction_spec) {
  # Build extraction queries
  extractions <- lapply(names(extraction_spec), function(col_name) {
    spec <- extraction_spec[[col_name]]
    sprintf("json_extract(data, '%s')::%s as %s", 
            spec$path, spec$type, col_name)
  })
  
  extraction_str <- paste(extractions, collapse = ",\n      ")
  
  DBI::dbExecute(con, sprintf("
    CREATE TABLE %s AS 
    SELECT 
      %s
    FROM (
      SELECT * FROM read_json_auto('%s') as data
    )
  ", table_name, extraction_str, file_path))
}

#' Export data to JSON format
#' 
#' @param con DuckDB connection
#' @param table_name Source table name
#' @param file_path Output file path
#' @param array Whether to export as JSON array
#' @export
export_json <- function(con, table_name, file_path, array = TRUE) {
  if (array) {
    # Export as JSON array
    DBI::dbExecute(con, sprintf("
      COPY (SELECT * FROM %s) TO '%s' (FORMAT JSON, ARRAY true)
    ", table_name, file_path))
  } else {
    # Export as newline-delimited JSON
    DBI::dbExecute(con, sprintf("
      COPY %s TO '%s' (FORMAT JSON, ARRAY false)
    ", table_name, file_path))
  }
}

# ============================================================================
# Section 4: Database Integration
# ============================================================================

#' PostgreSQL database integration
#' 
#' @param duckdb_con DuckDB connection
#' @param pg_config PostgreSQL connection configuration
#' @return List of available tables
#' @export
postgres_integration <- function(duckdb_con, pg_config) {
  # Install PostgreSQL extension
  tryCatch({
    DBI::dbExecute(duckdb_con, "INSTALL postgres")
  }, error = function(e) {})
  DBI::dbExecute(duckdb_con, "LOAD postgres")
  
  # Build connection string
  conn_str <- sprintf(
    "host=%s port=%s user=%s password=%s dbname=%s",
    pg_config$host %||% "localhost",
    pg_config$port %||% 5432,
    pg_config$user,
    pg_config$password,
    pg_config$dbname
  )
  
  # Attach PostgreSQL database
  DBI::dbExecute(duckdb_con, sprintf("
    ATTACH '%s' AS postgres_db (TYPE POSTGRES)
  ", conn_str))
  
  # Return available tables
  DBI::dbGetQuery(duckdb_con, "
    SELECT schemaname, tablename 
    FROM postgres_db.pg_tables 
    WHERE schemaname = 'public'
  ")
}

#' Copy table from PostgreSQL to DuckDB
#' 
#' @param con DuckDB connection with attached PostgreSQL
#' @param pg_table Source PostgreSQL table
#' @param duckdb_table Target DuckDB table
#' @param schema PostgreSQL schema (default: public)
#' @export
copy_from_postgres <- function(con, pg_table, duckdb_table, schema = "public") {
  DBI::dbExecute(con, sprintf("
    CREATE TABLE %s AS 
    SELECT * FROM postgres_db.%s.%s
  ", duckdb_table, schema, pg_table))
}

#' SQLite database integration
#' 
#' @param duckdb_con DuckDB connection
#' @param sqlite_path Path to SQLite database
#' @return List of available tables
#' @export
sqlite_integration <- function(duckdb_con, sqlite_path) {
  # Install SQLite extension
  tryCatch({
    DBI::dbExecute(duckdb_con, "INSTALL sqlite")
  }, error = function(e) {})
  DBI::dbExecute(duckdb_con, "LOAD sqlite")
  
  # Attach SQLite database
  DBI::dbExecute(duckdb_con, sprintf("
    ATTACH '%s' AS sqlite_db (TYPE SQLITE)
  ", sqlite_path))
  
  # List SQLite tables
  DBI::dbGetQuery(duckdb_con, "
    SELECT name FROM sqlite_db.sqlite_master 
    WHERE type='table'
  ")
}

#' Import all tables from SQLite
#' 
#' @param duckdb_con DuckDB connection with attached SQLite
#' @param prefix Optional prefix for imported tables
#' @export
import_all_sqlite_tables <- function(duckdb_con, prefix = "") {
  tables <- DBI::dbGetQuery(duckdb_con, "
    SELECT name FROM sqlite_db.sqlite_master 
    WHERE type='table'
  ")
  
  for (table in tables$name) {
    target_name <- paste0(prefix, table)
    DBI::dbExecute(duckdb_con, sprintf("
      CREATE TABLE %s AS 
      SELECT * FROM sqlite_db.%s
    ", target_name, table))
    cat("Imported table:", table, "->", target_name, "\n")
  }
}

# ============================================================================
# Section 5: Bulk Loading
# ============================================================================

#' High-performance bulk data loading
#' 
#' @param con DuckDB connection
#' @param file_pattern File pattern for bulk load
#' @param table_name Target table name
#' @param file_type File type (csv, parquet, json)
#' @export
bulk_load_data <- function(con, file_pattern, table_name, file_type = "csv") {
  # Configure for bulk loading
  DBI::dbExecute(con, "SET disable_checkpoint_on_shutdown=true")
  DBI::dbExecute(con, "SET checkpoint_threshold='1GB'")
  
  # Determine read function based on file type
  read_func <- switch(file_type,
    csv = "read_csv_auto",
    parquet = "read_parquet",
    json = "read_json_auto",
    stop("Unsupported file type")
  )
  
  # Create table from first file to get schema
  DBI::dbExecute(con, sprintf("
    CREATE TABLE %s AS 
    SELECT * FROM %s('%s') 
    LIMIT 0
  ", table_name, read_func, file_pattern))
  
  # Bulk insert with parallel processing
  DBI::dbExecute(con, sprintf("
    INSERT INTO %s 
    SELECT * FROM %s('%s', parallel=true)
  ", table_name, read_func, file_pattern))
  
  # Force checkpoint after bulk load
  DBI::dbExecute(con, "CHECKPOINT")
  
  # Get row count
  count <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(*) as n FROM %s", table_name
  ))$n
  
  cat("Loaded", count, "rows into", table_name, "\n")
}

#' Export large table in chunks
#' 
#' @param con DuckDB connection
#' @param table_name Source table
#' @param base_path Base output path
#' @param chunk_size Rows per file
#' @param format Output format (csv, parquet)
#' @export
export_chunked <- function(con, table_name, base_path, chunk_size = 1000000, format = "parquet") {
  # Get total row count
  total_rows <- DBI::dbGetQuery(con, 
    sprintf("SELECT COUNT(*) as n FROM %s", table_name)
  )$n
  
  num_chunks <- ceiling(total_rows / chunk_size)
  
  for (i in seq_len(num_chunks)) {
    offset <- (i - 1) * chunk_size
    file_path <- sprintf("%s_chunk_%04d.%s", base_path, i, format)
    
    query <- sprintf("
      SELECT * FROM %s 
      LIMIT %d OFFSET %d
    ", table_name, chunk_size, offset)
    
    if (format == "parquet") {
      DBI::dbExecute(con, sprintf("
        COPY (%s) TO '%s' (FORMAT PARQUET)
      ", query, file_path))
    } else if (format == "csv") {
      DBI::dbExecute(con, sprintf("
        COPY (%s) TO '%s' (FORMAT CSV, HEADER true)
      ", query, file_path))
    }
    
    cat("Exported chunk", i, "of", num_chunks, "to", file_path, "\n")
  }
}