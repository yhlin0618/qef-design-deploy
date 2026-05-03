#' @file fn_dbDisconnect_all.R
#' @use_package DBI
#'
#' @title Disconnect All Database Connections (General DBI Implementation)
#'
#' @description
#' Disconnects all DBI connections found in the global environment.
#' This is a general implementation that works with any DBI-compliant database.
#' For database-specific optimizations (like DuckDB export/backup operations), 
#' see the corresponding functions in specific subdirectories.
#'
#' @param verbose Logical. Display progress messages (default: TRUE)
#' @param remove_vars Logical. Remove connection variables after disconnect (default: TRUE)
#' @param skip_patterns Character vector. Variable name patterns to skip (default: c("db_path_list", "^\\.", "^temp_"))
#'
#' @return Integer. Number of connections successfully disconnected.
#'
#' @details
#' This function scans the global environment for DBI connection objects and
#' disconnects them safely. It:
#' 1. Identifies all valid DBI connections in .GlobalEnv
#' 2. Attempts to disconnect each connection
#' 3. Optionally removes the connection variables
#' 4. Reports success/failure counts
#'
#' For DuckDB databases requiring export/backup functionality, use
#' duckdb/fn_duckdb_disconnect_with_export.R instead.
#'
#' @examples
#' \dontrun{
#' # Connect to various databases
#' pg_con <- DBI::dbConnect(RPostgres::Postgres(), ...)
#' sqlite_con <- DBI::dbConnect(RSQLite::SQLite(), ...)
#' 
#' # Disconnect all
#' dbDisconnect_all()
#' 
#' # Disconnect but keep variables
#' dbDisconnect_all(remove_vars = FALSE)
#' }
#'
#' @export
#' @seealso duckdb/fn_duckdb_disconnect_with_export.R for DuckDB-specific features
dbDisconnect_all <- function(verbose = TRUE, 
                            remove_vars = TRUE,
                            skip_patterns = c("db_path_list", "^\\.", "^temp_")) {
  
  # Get all objects in global environment
  all_objects <- ls(envir = .GlobalEnv)
  
  # Filter out objects matching skip patterns
  objects_to_check <- all_objects
  for (pattern in skip_patterns) {
    objects_to_check <- objects_to_check[!grepl(pattern, objects_to_check)]
  }
  
  if (verbose && length(objects_to_check) > 0) {
    message("Scanning ", length(objects_to_check), " objects for DBI connections...")
  }
  
  success_count <- 0L
  failure_count <- 0L
  connection_vars <- character(0)
  
  # Check each object for DBI connection
  for (var_name in objects_to_check) {
    obj <- tryCatch(
      get(var_name, envir = .GlobalEnv), 
      error = function(e) NULL
    )
    
    # Skip if not a valid DBI connection
    if (is.null(obj) || !inherits(obj, "DBIConnection") || !DBI::dbIsValid(obj)) {
      next
    }
    
    connection_vars <- c(connection_vars, var_name)
    
    # Attempt to disconnect
    disconnect_success <- tryCatch({
      DBI::dbDisconnect(obj)
      if (verbose) {
        db_info <- tryCatch(DBI::dbGetInfo(obj), error = function(e) list(dbname = "unknown"))
        message("Disconnected: ", var_name, " (", class(obj)[1], ")")
      }
      TRUE
    }, error = function(e) {
      if (verbose) {
        message("Failed to disconnect '", var_name, "': ", e$message)
      }
      FALSE
    })
    
    # Update counters
    if (disconnect_success) {
      success_count <- success_count + 1L
      
      # Remove variable if requested
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
  
  # Summary message
  total_found <- length(connection_vars)
  if (verbose) {
    if (total_found == 0) {
      message("No DBI connections found in global environment")
    } else {
      message(sprintf("Connection summary: %d found, %d disconnected, %d failed", 
                     total_found, success_count, failure_count))
    }
  }
  
  invisible(success_count)
}

#' Close Specific Database Connection
#'
#' Safely disconnect a specific DBI connection with error handling.
#'
#' @param con DBI connection object
#' @param con_name Character. Name for the connection (for messages)
#' @param verbose Logical. Display messages
#'
#' @return Logical. TRUE if successful, FALSE otherwise
#'
#' @export
dbDisconnect_safe <- function(con, con_name = "connection", verbose = TRUE) {
  if (is.null(con) || !inherits(con, "DBIConnection")) {
    if (verbose) message("Not a valid DBI connection: ", con_name)
    return(FALSE)
  }
  
  if (!DBI::dbIsValid(con)) {
    if (verbose) message("Connection already invalid: ", con_name)
    return(TRUE)  # Consider this success since it's already disconnected
  }
  
  result <- tryCatch({
    DBI::dbDisconnect(con)
    if (verbose) message("Successfully disconnected: ", con_name)
    TRUE
  }, error = function(e) {
    if (verbose) message("Failed to disconnect '", con_name, "': ", e$message)
    FALSE
  })
  
  return(result)
}

#' List All DBI Connections in Environment
#'
#' Scan and list all DBI connections in the global environment.
#'
#' @param envir Environment to scan (default: .GlobalEnv)
#' @param include_invalid Logical. Include invalid/closed connections
#'
#' @return Data frame with connection information
#'
#' @export
list_dbi_connections <- function(envir = .GlobalEnv, include_invalid = FALSE) {
  all_objects <- ls(envir = envir)
  connections <- data.frame(
    variable = character(0),
    class = character(0),
    valid = logical(0),
    database = character(0),
    stringsAsFactors = FALSE
  )
  
  for (var_name in all_objects) {
    obj <- tryCatch(get(var_name, envir = envir), error = function(e) NULL)
    
    if (is.null(obj) || !inherits(obj, "DBIConnection")) next
    
    is_valid <- DBI::dbIsValid(obj)
    
    if (!include_invalid && !is_valid) next
    
    # Get database info
    db_name <- tryCatch({
      info <- DBI::dbGetInfo(obj)
      info$dbname %||% info$dbdir %||% "unknown"
    }, error = function(e) "unknown")
    
    connections <- rbind(connections, data.frame(
      variable = var_name,
      class = class(obj)[1],
      valid = is_valid,
      database = db_name,
      stringsAsFactors = FALSE
    ))
  }
  
  return(connections)
}
