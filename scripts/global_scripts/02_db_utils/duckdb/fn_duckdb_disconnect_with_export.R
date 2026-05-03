#' DuckDB-Specific Disconnect with Export/Backup Operations
#'
#' Advanced DuckDB disconnection functionality that includes database export,
#' backup creation, and WAL file cleanup. This provides enhanced data integrity
#' and persistence features specific to DuckDB's file-based architecture.
#'
#' @file fn_duckdb_disconnect_with_export.R
#' @use_package DBI
#' @use_package duckdb
#' @use_package uuid
#' @date 2025-08-28
#'
#' @seealso fn_dbDisconnect_all.R for general DBI disconnect functionality

library(DBI)

# Helper functions ------------------------------------------------------------

#' Check if DuckDB version supports EXPORT DATABASE command
#' @return Logical indicating support
.duckdb_supports_export <- function() {
  ver <- tryCatch(numeric_version(duckdb::duckdb_version()), error = function(e) "0.0.0")
  ver >= "0.9.0"
}

#' Get catalog name from DuckDB connection
#' @param con DuckDB connection
#' @return Character string with catalog name
.get_catalog_name <- function(con) {
  res <- DBI::dbGetQuery(con, "PRAGMA database_list;")
  if (nrow(res)) res$name[1] else "memory"
}

#' Generate random alias for temporary database attachment
#' @return Character string with random alias
.random_alias <- function() paste0("db_", substr(gsub("-", "", uuid::UUIDgenerate()), 1, 8))

# Define null-coalescing operator if not available
if (!exists("%||%")) {
  `%||%` <- function(a, b) if (is.null(a)) b else a
}

# Main functions --------------------------------------------------------------

#' Disconnect DuckDB Connections with Export and Backup
#'
#' Enhanced disconnection for DuckDB connections that includes:
#' 1. Database export using EXPORT DATABASE or COPY FROM DATABASE
#' 2. File replacement with backup creation
#' 3. WAL file cleanup
#' 4. Read-only connection detection and handling
#'
#' @param verbose Logical. Display progress messages (default: TRUE)
#' @param remove_vars Logical. Remove connection variables after disconnect (default: TRUE)
#' @param create_backups Logical. Create backup files before replacement (default: TRUE)
#' @param keep_backups Logical. Keep backup files after successful replacement (default: FALSE)
#' @param cleanup_wal Logical. Remove old WAL files (default: TRUE)
#' @param backup_suffix Character. Suffix for backup files (default: "_bak")
#' @param export_dir Character. Directory for temporary export files (default: tempdir())
#' @param include_general_dbi Logical. Also disconnect non-DuckDB connections (default: TRUE)
#'
#' @return Integer. Number of connections successfully processed
#'
#' @details
#' This function specifically handles DuckDB connections with advanced features:
#' 
#' For each DuckDB connection:
#' 1. Detects read-only status to skip export operations
#' 2. Attempts EXPORT DATABASE command (DuckDB 0.9+)
#' 3. Falls back to COPY FROM DATABASE for older versions
#' 4. Creates timestamped backups before file replacement
#' 5. Replaces original database with exported version
#' 6. Cleans up WAL files and optionally removes backups
#' 
#' For non-DuckDB connections (if include_general_dbi = TRUE):
#' - Simple disconnection using general DBI interface
#' 
#' This ensures data persistence and integrity for file-based DuckDB databases
#' while providing a clean shutdown process.
#'
#' @examples
#' \dontrun{
#' # Standard DuckDB disconnect with export
#' duckdb_disconnect_with_export()
#' 
#' # Keep backups and don't clean WAL files
#' duckdb_disconnect_with_export(
#'   keep_backups = TRUE,
#'   cleanup_wal = FALSE
#' )
#' 
#' # Only DuckDB connections, skip general DBI
#' duckdb_disconnect_with_export(include_general_dbi = FALSE)
#' }
#'
#' @export
duckdb_disconnect_with_export <- function(verbose = TRUE,
                                         remove_vars = TRUE,
                                         create_backups = TRUE,
                                         keep_backups = FALSE,
                                         cleanup_wal = TRUE,
                                         backup_suffix = "_bak",
                                         export_dir = tempdir(),
                                         include_general_dbi = TRUE) {
  
  # Ensure export directory exists
  dir.create(export_dir, showWarnings = FALSE, recursive = TRUE)
  
  # Get all objects in global environment
  all_objects <- ls(envir = .GlobalEnv)
  
  # Skip system objects
  objects_to_check <- all_objects[!all_objects %in% c("db_path_list") & !startsWith(all_objects, ".")]
  
  success_count <- 0L
  failure_count <- 0L
  duckdb_count <- 0L
  general_count <- 0L
  
  export_supported <- .duckdb_supports_export()
  
  if (verbose) {
    message("DuckDB disconnect with export/backup - version support: ", 
           ifelse(export_supported, "EXPORT DATABASE available", "Using COPY FROM DATABASE fallback"))
  }
  
  for (var_name in objects_to_check) {
    con <- tryCatch(get(var_name, .GlobalEnv), error = function(e) NULL)
    
    # Skip if not a valid DBI connection
    if (is.null(con) || !inherits(con, "DBIConnection") || !DBI::dbIsValid(con)) {
      next
    }
    
    is_duckdb <- inherits(con, "duckdb_connection")
    
    if (is_duckdb) {
      duckdb_count <- duckdb_count + 1L
      result <- .process_duckdb_connection(
        con, var_name, export_supported, verbose, create_backups,
        keep_backups, cleanup_wal, backup_suffix, export_dir
      )
    } else if (include_general_dbi) {
      general_count <- general_count + 1L
      result <- .process_general_connection(con, var_name, verbose)
    } else {
      next  # Skip non-DuckDB connections
    }
    
    # Handle disconnection result
    if (result) {
      success_count <- success_count + 1L
      
      if (remove_vars) {
        tryCatch({
          rm(list = var_name, envir = .GlobalEnv)
          if (verbose) message("Removed variable: ", var_name)
        }, error = function(e) {
          if (verbose) message("Failed to remove variable '", var_name, "': ", e$message)
        })
      }
    } else {
      failure_count <- failure_count + 1L
    }
  }
  
  # Summary
  if (verbose) {
    total_processed <- success_count + failure_count
    message(sprintf(
      "Disconnect summary: %d total processed (%d DuckDB, %d general), %d successful, %d failed",
      total_processed, duckdb_count, general_count, success_count, failure_count
    ))
  }
  
  invisible(success_count)
}

#' Process DuckDB Connection with Export/Backup
#' @param con DuckDB connection
#' @param var_name Variable name
#' @param export_supported Whether EXPORT DATABASE is supported
#' @param verbose Verbose messaging
#' @param create_backups Create backup files
#' @param keep_backups Keep backup files
#' @param cleanup_wal Clean WAL files
#' @param backup_suffix Backup file suffix
#' @param export_dir Export directory
#' @return Logical success indicator
.process_duckdb_connection <- function(con, var_name, export_supported, verbose,
                                      create_backups, keep_backups, cleanup_wal,
                                      backup_suffix, export_dir) {
  
  # Get database file path
  db_info <- DBI::dbGetInfo(con)
  db_path <- db_info$dbname
  
  if (is.null(db_path) || is.na(db_path)) {
    if (verbose) message("No file path for DuckDB connection: ", var_name)
    return(.simple_disconnect(con, var_name, verbose))
  }
  
  # Check if connection is read-only
  read_only <- .detect_readonly(con, verbose)
  
  export_success <- FALSE
  backup_path <- NULL
  temp_export_path <- NULL
  
  # Export process (skip if read-only)
  if (!read_only) {
    temp_export_path <- file.path(export_dir, 
                                 paste0(tools::file_path_sans_ext(basename(db_path)), "_export.duckdb"))
    
    # Remove existing temp file
    if (file.exists(temp_export_path)) {
      file.remove(temp_export_path)
    }
    
    # Try export methods
    export_success <- .export_database(con, temp_export_path, export_supported, verbose)
    
    if (!export_success && verbose) {
      warning("Failed to export database '", var_name, "', skipping file replacement")
    }
  } else {
    export_success <- TRUE  # Skip export for read-only
    if (verbose) message("Read-only connection '", var_name, "', skipping export")
  }
  
  # Disconnect first (required before file operations)
  disconnect_success <- .disconnect_with_cleanup(con, var_name, verbose)
  
  # File replacement and cleanup (only if export succeeded and not read-only)
  if (export_success && !read_only && !is.null(temp_export_path) && file.exists(temp_export_path)) {
    .replace_database_file(db_path, temp_export_path, backup_path, create_backups,
                          keep_backups, cleanup_wal, backup_suffix, verbose)
  }
  
  return(disconnect_success)
}

#' Process General DBI Connection
#' @param con DBI connection
#' @param var_name Variable name
#' @param verbose Verbose messaging
#' @return Logical success indicator
.process_general_connection <- function(con, var_name, verbose) {
  return(.simple_disconnect(con, var_name, verbose))
}

#' Simple disconnect operation
#' @param con DBI connection
#' @param var_name Variable name
#' @param verbose Verbose messaging
#' @return Logical success indicator
.simple_disconnect <- function(con, var_name, verbose) {
  tryCatch({
    DBI::dbDisconnect(con)
    if (verbose) message("Disconnected: ", var_name, " (", class(con)[1], ")")
    TRUE
  }, error = function(e) {
    if (verbose) message("Failed to disconnect '", var_name, "': ", e$message)
    FALSE
  })
}

#' Detect if DuckDB connection is read-only
#' @param con DuckDB connection
#' @param verbose Verbose messaging
#' @return Logical indicating read-only status
.detect_readonly <- function(con, verbose) {
  tryCatch({
    # Multiple methods to detect read-only
    info <- DBI::dbGetInfo(con)
    
    # Check driver read-only flag
    if (isTRUE(con@driver@read_only)) return(TRUE)
    
    # Check database name for read-only indicators
    if (grepl("read.only", tolower(info$dbname), fixed = TRUE)) return(TRUE)
    
    # Test with temporary table creation
    test_result <- tryCatch({
      DBI::dbExecute(con, "CREATE TEMP TABLE __test_readonly__ AS SELECT 1")
      DBI::dbExecute(con, "DROP TABLE __test_readonly__")
      FALSE  # Successfully created/dropped, not read-only
    }, error = function(e) {
      grepl("read.only", e$message, ignore.case = TRUE) || 
      grepl("database is locked", e$message, ignore.case = TRUE)
    })
    
    return(test_result)
    
  }, error = function(e) {
    if (verbose) message("Could not determine read-only status for connection, assuming writable")
    FALSE
  })
}

#' Export DuckDB database using best available method
#' @param con DuckDB connection
#' @param temp_path Temporary export file path
#' @param export_supported Whether EXPORT DATABASE is supported
#' @param verbose Verbose messaging
#' @return Logical success indicator
.export_database <- function(con, temp_path, export_supported, verbose) {
  
  # Method 1: EXPORT DATABASE (DuckDB 0.9+)
  if (export_supported) {
    sql <- sprintf("EXPORT DATABASE '%s' (FORMAT duckdb);", temp_path)
    export_success <- tryCatch({
      DBI::dbExecute(con, sql)
      if (verbose) message("Successfully exported using EXPORT DATABASE")
      TRUE
    }, error = function(e) {
      if (verbose) message("EXPORT DATABASE failed: ", e$message)
      FALSE
    })
    
    if (export_success) return(TRUE)
  }
  
  # Method 2: COPY FROM DATABASE (fallback)
  alias <- .random_alias()
  src_catalog <- .get_catalog_name(con)
  
  sql <- sprintf(
    "DETACH DATABASE IF EXISTS %s; ATTACH '%s' AS %s; COPY FROM DATABASE %s TO %s; DETACH DATABASE %s;",
    alias, temp_path, alias, DBI::dbQuoteIdentifier(con, src_catalog), alias, alias
  )
  
  fallback_success <- tryCatch({
    DBI::dbExecute(con, sql)
    if (verbose) message("Successfully exported using COPY FROM DATABASE")
    TRUE
  }, error = function(e) {
    if (verbose) message("COPY FROM DATABASE failed: ", e$message)
    FALSE
  })
  
  return(fallback_success)
}

#' Disconnect with attached database cleanup
#' @param con DuckDB connection
#' @param var_name Variable name
#' @param verbose Verbose messaging
#' @return Logical success indicator
.disconnect_with_cleanup <- function(con, var_name, verbose) {
  tryCatch({
    # Clean up any attached databases first
    if (exists("dbDetach_all", envir = .GlobalEnv)) {
      tryCatch({
        dbDetach_all(con, verbose = FALSE)  # Suppress verbose for cleanup
      }, error = function(e) {
        if (verbose) message("Warning: Could not detach databases before disconnect: ", e$message)
      })
    }
    
    # Disconnect main connection
    DBI::dbDisconnect(con)
    if (verbose) message("Disconnected DuckDB connection: ", var_name)
    TRUE
    
  }, error = function(e) {
    if (verbose) message("Failed to disconnect '", var_name, "': ", e$message)
    FALSE
  })
}

#' Replace database file with exported version
#' @param original_path Original database file path
#' @param export_path Exported database file path
#' @param backup_path Backup file path (will be set)
#' @param create_backups Whether to create backups
#' @param keep_backups Whether to keep backups
#' @param cleanup_wal Whether to clean WAL files
#' @param backup_suffix Backup file suffix
#' @param verbose Verbose messaging
.replace_database_file <- function(original_path, export_path, backup_path,
                                  create_backups, keep_backups, cleanup_wal,
                                  backup_suffix, verbose) {
  
  # Create backup
  if (create_backups && file.exists(original_path)) {
    timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    backup_path <- paste0(original_path, backup_suffix, timestamp)
    
    if (file.copy(original_path, backup_path, overwrite = FALSE)) {
      if (verbose) message("Created backup: ", basename(backup_path))
    } else {
      if (verbose) warning("Failed to create backup file")
      return(FALSE)
    }
  }
  
  # Replace original with exported version
  if (file.rename(export_path, original_path)) {
    if (verbose) message("Replaced database with exported version: ", basename(original_path))
    
    # Remove backup if not keeping
    if (!keep_backups && !is.null(backup_path) && file.exists(backup_path)) {
      if (file.remove(backup_path)) {
        if (verbose) message("Removed backup file: ", basename(backup_path))
      } else {
        if (verbose) warning("Failed to remove backup file: ", basename(backup_path))
      }
    }
    
    # Clean up WAL files
    if (cleanup_wal) {
      wal_file <- paste0(original_path, ".wal")
      if (file.exists(wal_file)) {
        if (file.remove(wal_file)) {
          if (verbose) message("Removed WAL file: ", basename(wal_file))
        } else {
          if (verbose) warning("Failed to remove WAL file: ", basename(wal_file))
        }
      }
    }
    
    return(TRUE)
    
  } else {
    if (verbose) warning("Failed to replace database file with exported version")
    return(FALSE)
  }
}