#' DuckDB-Optimized Table Copy Operations
#'
#' This file provides DuckDB-specific optimized table copy operations using
#' DuckDB's native ATTACH and COPY commands for maximum performance.
#'
#' @file fn_duckdb_copy_optimized.R
#' @use_package DBI
#' @use_package duckdb
#' @date 2025-08-28
#'
#' @seealso fn_dbCopyTable.R for general DBI implementation

library(DBI)

#' Copy Table Between DuckDB Databases (Optimized)
#'
#' High-performance table copying between DuckDB databases using native
#' ATTACH/DETACH operations. This avoids loading data into R memory and
#' provides significant performance benefits for large tables.
#'
#' @param source_con DuckDB connection object for source database
#' @param target_con DuckDB connection object for target database  
#' @param source_table Character. Name of table in source database
#' @param target_table Character. Name for table in target database (default: same as source)
#' @param overwrite Logical. Whether to overwrite existing target table (default: FALSE)
#' @param read_only Logical. Whether to attach target as read-only (default: FALSE)
#'
#' @return Invisible TRUE on success
#'
#' @details
#' This function uses DuckDB's ATTACH command to temporarily attach the target
#' database to the source connection, then performs a direct CREATE TABLE AS SELECT
#' operation. This is much faster than the general dbCopyTable() function for
#' large datasets as it never loads data into R memory.
#'
#' @examples
#' \dontrun{
#' source_db <- DBI::dbConnect(duckdb::duckdb(), "source.duckdb")
#' target_db <- DBI::dbConnect(duckdb::duckdb(), "target.duckdb")
#' 
#' # Fast copy using DuckDB optimization
#' duckdb_copy_table_optimized(source_db, target_db, "large_table")
#' 
#' DBI::dbDisconnect(source_db)
#' DBI::dbDisconnect(target_db)
#' }
#'
#' @export
duckdb_copy_table_optimized <- function(source_con, target_con, source_table, 
                                       target_table = source_table,
                                       overwrite = FALSE, read_only = FALSE) {
  
  # Validate DuckDB connections
  if (!inherits(source_con, "duckdb_connection") || !inherits(target_con, "duckdb_connection")) {
    stop("Both connections must be DuckDB connections. Use general dbCopyTable() for mixed databases.")
  }
  
  if (!DBI::dbIsValid(source_con) || !DBI::dbIsValid(target_con)) {
    stop("Invalid DuckDB connection(s)")
  }
  
  # Get database paths
  src_info <- DBI::dbGetInfo(source_con)
  tgt_info <- DBI::dbGetInfo(target_con)
  
  if (is.null(src_info$dbdir) || is.null(tgt_info$dbdir)) {
    stop("Cannot get database file paths for ATTACH operation")
  }
  
  src_path <- normalizePath(src_info$dbdir, mustWork = FALSE)
  tgt_path <- normalizePath(tgt_info$dbdir, mustWork = FALSE)
  
  # Check if trying to copy within same database
  if (src_path == tgt_path) {
    message("Source and target are same database - performing internal copy")
    if (source_table == target_table) {
      stop("Cannot copy table to itself within same database")
    }
    
    # Internal copy within same database
    if (overwrite) {
      DBI::dbExecute(source_con, sprintf("DROP TABLE IF EXISTS %s", 
                                         DBI::SQL(target_table)))
    }
    
    DBI::dbExecute(source_con, sprintf("CREATE TABLE %s AS SELECT * FROM %s",
                                       DBI::SQL(target_table), DBI::SQL(source_table)))
    message(sprintf("Internal copy completed: %s -> %s", source_table, target_table))
    return(invisible(TRUE))
  }
  
  # Check if source table exists
  if (!source_table %in% DBI::dbListTables(source_con)) {
    stop("Table '", source_table, "' does not exist in source database")
  }
  
  # Generate random alias for target database to avoid conflicts
  target_alias <- paste0("tgt_", substr(gsub("-", "", uuid::UUIDgenerate()), 1, 8))
  
  message(sprintf("Performing optimized DuckDB copy: %s -> %s via ATTACH", 
                  source_table, target_table))
  
  tryCatch({
    # Attach target database
    attach_options <- if (read_only) " (READ_ONLY)" else ""
    attach_sql <- sprintf("ATTACH '%s' AS %s%s", tgt_path, target_alias, attach_options)
    DBI::dbExecute(source_con, attach_sql)
    
    # Handle overwrite
    if (overwrite) {
      drop_sql <- sprintf("DROP TABLE IF EXISTS %s.%s", 
                          target_alias, DBI::SQL(target_table))
      DBI::dbExecute(source_con, drop_sql)
    }
    
    # Perform the copy
    copy_sql <- sprintf("CREATE TABLE %s.%s AS SELECT * FROM main.%s",
                        target_alias, DBI::SQL(target_table), DBI::SQL(source_table))
    DBI::dbExecute(source_con, copy_sql)
    
    message(sprintf("Successfully copied table %s -> %s (optimized)", 
                    source_table, target_table))
    
  }, finally = {
    # Always detach, even if copy failed
    tryCatch({
      DBI::dbExecute(source_con, sprintf("DETACH %s", target_alias))
    }, error = function(e) {
      warning("Failed to detach target database: ", e$message)
    })
  })
  
  invisible(TRUE)
}

#' Copy Multiple Tables Between DuckDB Databases
#'
#' Efficiently copy multiple tables using DuckDB's ATTACH mechanism.
#' The target database is attached once and multiple tables are copied.
#'
#' @param source_con DuckDB connection object for source database
#' @param target_con DuckDB connection object for target database
#' @param table_list Character vector of table names to copy
#' @param table_mapping Named character vector for renaming tables (optional)
#' @param overwrite Logical. Whether to overwrite existing tables (default: FALSE)
#' @param read_only Logical. Whether to attach as read-only (default: FALSE)
#'
#' @return Named logical vector indicating success/failure for each table
#'
#' @examples
#' \dontrun{
#' # Copy multiple tables
#' duckdb_copy_multiple_tables(
#'   source_con = source_db,
#'   target_con = target_db,
#'   table_list = c("customers", "orders", "products")
#' )
#' 
#' # Copy with renaming
#' duckdb_copy_multiple_tables(
#'   source_con = source_db,
#'   target_con = target_db, 
#'   table_list = c("old_customers", "old_orders"),
#'   table_mapping = c("old_customers" = "customers", "old_orders" = "orders"),
#'   overwrite = TRUE
#' )
#' }
#'
#' @export
duckdb_copy_multiple_tables <- function(source_con, target_con, table_list,
                                       table_mapping = NULL, overwrite = FALSE,
                                       read_only = FALSE) {
  
  # Validate inputs
  if (!inherits(source_con, "duckdb_connection") || !inherits(target_con, "duckdb_connection")) {
    stop("Both connections must be DuckDB connections")
  }
  
  if (length(table_list) == 0) {
    stop("table_list cannot be empty")
  }
  
  # Get database paths
  src_info <- DBI::dbGetInfo(source_con)
  tgt_info <- DBI::dbGetInfo(target_con)
  tgt_path <- normalizePath(tgt_info$dbdir, mustWork = FALSE)
  target_alias <- paste0("tgt_", substr(gsub("-", "", uuid::UUIDgenerate()), 1, 8))
  
  results <- setNames(logical(length(table_list)), table_list)
  
  tryCatch({
    # Attach target database once
    attach_options <- if (read_only) " (READ_ONLY)" else ""
    attach_sql <- sprintf("ATTACH '%s' AS %s%s", tgt_path, target_alias, attach_options)
    DBI::dbExecute(source_con, attach_sql)
    
    message(sprintf("Attached target database as %s", target_alias))
    
    # Copy each table
    for (table in table_list) {
      target_name <- if (is.null(table_mapping)) table else table_mapping[table]
      if (is.na(target_name)) target_name <- table
      
      results[table] <- tryCatch({
        # Check if source table exists
        if (!table %in% DBI::dbListTables(source_con)) {
          warning("Table '", table, "' not found in source database")
          FALSE
        } else {
          # Handle overwrite
          if (overwrite) {
            drop_sql <- sprintf("DROP TABLE IF EXISTS %s.%s", 
                                target_alias, DBI::SQL(target_name))
            DBI::dbExecute(source_con, drop_sql)
          }
          
          # Copy table
          copy_sql <- sprintf("CREATE TABLE %s.%s AS SELECT * FROM main.%s",
                              target_alias, DBI::SQL(target_name), DBI::SQL(table))
          DBI::dbExecute(source_con, copy_sql)
          
          message(sprintf("Copied: %s -> %s", table, target_name))
          TRUE
        }
      }, error = function(e) {
        warning("Failed to copy table '", table, "': ", e$message)
        FALSE
      })
    }
    
  }, finally = {
    # Always detach
    tryCatch({
      DBI::dbExecute(source_con, sprintf("DETACH %s", target_alias))
      message("Detached target database")
    }, error = function(e) {
      warning("Failed to detach target database: ", e$message)
    })
  })
  
  # Summary
  success_count <- sum(results)
  total_count <- length(results)
  message(sprintf("Copied %d/%d tables successfully", success_count, total_count))
  
  invisible(results)
}

#' Copy Database Schema (Tables Only)
#'
#' Copy all tables from source DuckDB to target DuckDB using optimized ATTACH method.
#'
#' @param source_con DuckDB connection for source database
#' @param target_con DuckDB connection for target database
#' @param exclude_tables Character vector of table names to exclude
#' @param overwrite Logical. Whether to overwrite existing tables
#'
#' @return Named logical vector indicating success for each table
#'
#' @export
duckdb_copy_database_schema <- function(source_con, target_con, 
                                       exclude_tables = character(0),
                                       overwrite = FALSE) {
  
  all_tables <- DBI::dbListTables(source_con)
  tables_to_copy <- setdiff(all_tables, exclude_tables)
  
  if (length(tables_to_copy) == 0) {
    message("No tables to copy")
    return(logical(0))
  }
  
  message(sprintf("Copying %d tables from source to target database", 
                  length(tables_to_copy)))
  
  duckdb_copy_multiple_tables(
    source_con = source_con,
    target_con = target_con,
    table_list = tables_to_copy,
    overwrite = overwrite
  )
}