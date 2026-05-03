#' Copy Table Between Database Connections (General DBI Implementation)
#'
#' This function copies a table from one database connection to another using
#' only the standard DBI interface. It reads the data into memory and writes
#' it to the target database. For database-specific optimizations, see the
#' corresponding functions in specific subdirectories.
#'
#' @param source_con A DBI connection object for the source database
#' @param target_con A DBI connection object for the target database
#' @param source_table Character string. Name of the table to copy from source database
#' @param target_table Character string. Name of the table to create in target database (default: same as source_table)
#' @param overwrite Logical. Whether to overwrite existing table in target database (default: FALSE)
#' @param temporary Logical. Whether to create a temporary table in target database (default: FALSE)
#' @param chunk_size Integer. For large tables, read in chunks (default: NULL for all at once)
#'
#' @return Invisible NULL. The function creates a table as a side effect.
#'
#' @details
#' This function performs a complete copy of a table by:
#' 1. Reading all data from the source table into memory using DBI::dbReadTable()
#' 2. Writing the data to the target database using DBI::dbWriteTable()
#' 
#' Note: For large tables, this operation may consume significant memory as all
#' data is loaded into R before being written to the target database. For 
#' database-specific optimizations (like DuckDB ATTACH operations), use the
#' specialized functions in the respective subdirectories.
#'
#' @examples
#' \dontrun{
#' # Copy table with same name
#' dbCopyTable(
#'   source_con = postgres_con,
#'   target_con = mysql_con,
#'   source_table = "customers"
#' )
#' 
#' # Copy table with different name and overwrite if exists
#' dbCopyTable(
#'   source_con = sqlite_con,
#'   target_con = postgres_con,
#'   source_table = "old_products",
#'   target_table = "new_products",
#'   overwrite = TRUE
#' )
#' 
#' # Copy as temporary table
#' dbCopyTable(
#'   source_con = source_db,
#'   target_con = temp_db,
#'   source_table = "large_table",
#'   temporary = TRUE
#' )
#' }
#'
#' @export
#' @seealso duckdb/fn_duckdb_copy_optimized.R for DuckDB-specific optimizations
dbCopyTable <- function(source_con, target_con, source_table, target_table = source_table,
                        overwrite = FALSE, temporary = FALSE, chunk_size = NULL) {

  # Validate connections
  if (!DBI::dbIsValid(source_con) || !DBI::dbIsValid(target_con)) {
    stop("Invalid DBI connection(s)")
  }

  # Check if source table exists
  if (!source_table %in% DBI::dbListTables(source_con)) {
    stop("Table '", source_table, "' does not exist in source database")
  }

  message(sprintf("Copying table %s -> %s using general DBI interface...", 
                  source_table, target_table))

  # Read data from source
  if (is.null(chunk_size)) {
    # Read all at once
    data <- DBI::dbReadTable(source_con, source_table)
    
    # Write to target
    DBI::dbWriteTable(target_con, target_table, data, 
                      overwrite = overwrite, temporary = temporary)
  } else {
    # Chunked reading (for very large tables)
    # Note: This is a basic implementation - specific databases may have better approaches
    total_rows <- DBI::dbGetQuery(source_con, 
                                  paste("SELECT COUNT(*) as n FROM", source_table))$n
    
    if (overwrite && DBI::dbExistsTable(target_con, target_table)) {
      DBI::dbRemoveTable(target_con, target_table)
    }
    
    first_chunk <- TRUE
    for (offset in seq(0, total_rows - 1, chunk_size)) {
      chunk_query <- sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", 
                             source_table, chunk_size, offset)
      chunk_data <- DBI::dbGetQuery(source_con, chunk_query)
      
      if (nrow(chunk_data) == 0) break
      
      DBI::dbWriteTable(target_con, target_table, chunk_data, 
                        append = !first_chunk, temporary = temporary)
      first_chunk <- FALSE
      
      message(sprintf("Copied %d/%d rows", min(offset + chunk_size, total_rows), total_rows))
    }
  }

  message("Copy completed using general DBI interface")
  invisible(TRUE)
}