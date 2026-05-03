# ============================================================================
# DuckDB Connection Management Functions
# ============================================================================
# Purpose: Connection handling and resource management for DuckDB
# Source: Extracted from DU02_connection_management.qmd
# Reference: docs/en/part2_implementations/CH17_database_specifications/duckdb/DU02_connection_management.qmd
# Created: 2025-08-28
# ============================================================================

library(DBI)
library(duckdb)
library(yaml)
library(R6)

# ============================================================================
# Section 1: Basic Connection Functions
# ============================================================================

#' Load configured DuckDB connection from YAML
#' 
#' @param config_path Path to YAML configuration file
#' @return DuckDB connection object
#' @export
load_configured_connection <- function(config_path = "app_config.yaml") {
  config <- yaml::read_yaml(config_path)
  db_config <- config$database
  
  # Create connection
  con <- dbConnect(
    duckdb::duckdb(),
    dbdir = db_config$path,
    read_only = db_config$read_only %||% FALSE
  )
  
  # Apply configuration settings
  if (!is.null(db_config$memory_limit)) {
    dbExecute(con, sprintf("SET memory_limit='%s'", db_config$memory_limit))
  }
  
  if (!is.null(db_config$threads)) {
    dbExecute(con, sprintf("SET threads=%d", db_config$threads))
  }
  
  if (!is.null(db_config$temp_directory)) {
    dbExecute(con, sprintf("SET temp_directory='%s'", db_config$temp_directory))
  }
  
  if (!is.null(db_config$preserve_insertion_order)) {
    dbExecute(con, sprintf("SET preserve_insertion_order=%s", 
                          tolower(as.character(db_config$preserve_insertion_order))))
  }
  
  return(con)
}

#' Execute expression with auto-cleanup connection
#' 
#' @param db_path Path to DuckDB database file
#' @param expr Expression to execute with connection
#' @return Result of expression
#' @export
with_duckdb_connection <- function(db_path, expr) {
  con <- dbConnect(duckdb::duckdb(), dbdir = db_path)
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  
  eval(substitute(expr), envir = list(con = con), enclos = parent.frame())
}

#' Connect with retry logic
#' 
#' @param db_path Path to database file
#' @param max_retries Maximum retry attempts
#' @param wait_time Base wait time between retries
#' @return DuckDB connection object
#' @export
connect_with_retry <- function(db_path, max_retries = 3, wait_time = 1) {
  for (attempt in 1:max_retries) {
    con <- tryCatch({
      dbConnect(duckdb::duckdb(), dbdir = db_path)
    }, error = function(e) {
      if (attempt < max_retries) {
        message(sprintf("Connection attempt %d failed: %s. Retrying...", 
                       attempt, e$message))
        Sys.sleep(wait_time * attempt)  # Exponential backoff
        NULL
      } else {
        stop(sprintf("Failed to connect after %d attempts: %s", 
                    max_retries, e$message))
      }
    })
    
    if (!is.null(con)) {
      return(con)
    }
  }
}

# ============================================================================
# Section 2: Connection Pool Implementation
# ============================================================================

#' R6 Class for DuckDB Connection Pool
#' 
#' @export
DuckDBPool <- R6::R6Class("DuckDBPool",
  public = list(
    db_path = NULL,
    max_connections = NULL,
    connections = NULL,
    in_use = NULL,
    
    initialize = function(db_path, max_connections = 5) {
      self$db_path <- db_path
      self$max_connections <- max_connections
      self$connections <- list()
      self$in_use <- logical(max_connections)
    },
    
    get_connection = function() {
      # Find available connection
      for (i in seq_along(self$connections)) {
        if (!self$in_use[i]) {
          self$in_use[i] <- TRUE
          return(self$connections[[i]])
        }
      }
      
      # Create new connection if under limit
      if (length(self$connections) < self$max_connections) {
        con <- dbConnect(duckdb::duckdb(), dbdir = self$db_path)
        self$connections[[length(self$connections) + 1]] <- con
        self$in_use[length(self$connections)] <- TRUE
        return(con)
      }
      
      stop("No connections available in pool")
    },
    
    return_connection = function(con) {
      for (i in seq_along(self$connections)) {
        if (identical(self$connections[[i]], con)) {
          self$in_use[i] <- FALSE
          return(invisible(TRUE))
        }
      }
      warning("Connection not found in pool")
    },
    
    close_all = function() {
      for (con in self$connections) {
        try(dbDisconnect(con, shutdown = TRUE), silent = TRUE)
      }
      self$connections <- list()
      self$in_use <- logical(0)
    }
  )
)

# ============================================================================
# Section 3: Configuration Functions
# ============================================================================

#' Configure memory settings for DuckDB connection
#' 
#' @param con DuckDB connection
#' @param config List with memory configuration
#' @export
configure_memory <- function(con, config = list()) {
  # Set memory limit (default 80% of system memory)
  memory_limit <- config$memory_limit %||% "80%"
  dbExecute(con, sprintf("SET memory_limit='%s'", memory_limit))
  
  # Set max memory per query
  if (!is.null(config$max_memory_per_query)) {
    dbExecute(con, sprintf("SET max_memory='%s'", config$max_memory_per_query))
  }
  
  # Enable/disable external sorting
  external_threads <- config$external_threads %||% 1
  dbExecute(con, sprintf("SET external_threads=%d", external_threads))
  
  # Set temp directory for spilling
  if (!is.null(config$temp_directory)) {
    dbExecute(con, sprintf("SET temp_directory='%s'", config$temp_directory))
  }
}

#' Optimize performance settings
#' 
#' @param con DuckDB connection
#' @export
optimize_performance <- function(con) {
  # Set number of threads
  threads <- parallel::detectCores() - 1
  dbExecute(con, sprintf("SET threads=%d", threads))
  
  # Enable parallelism
  dbExecute(con, "SET enable_parallel=true")
  
  # Optimize for analytical workloads
  dbExecute(con, "SET default_order_by_nulls_last=true")
  dbExecute(con, "SET enable_object_cache=true")
  
  # Set checkpoint threshold for WAL
  dbExecute(con, "SET checkpoint_threshold='1GB'")
}

# ============================================================================
# Section 4: Health Monitoring
# ============================================================================

#' Check connection health
#' 
#' @param con DuckDB connection
#' @return List with health status information
#' @export
check_connection_health <- function(con) {
  tryCatch({
    # Test basic query
    result <- dbGetQuery(con, "SELECT 1 as test")
    
    # Check database statistics
    stats <- dbGetQuery(con, "SELECT * FROM duckdb_databases()")
    
    # Get memory usage
    memory <- dbGetQuery(con, "SELECT * FROM duckdb_memory()")
    
    list(
      status = "healthy",
      test_query = result$test == 1,
      database_info = stats,
      memory_usage = memory
    )
  }, error = function(e) {
    list(
      status = "unhealthy",
      error = e$message
    )
  })
}

#' Monitor connection metrics
#' 
#' @param con DuckDB connection
#' @return List with connection metrics
#' @export
monitor_connection <- function(con) {
  # Get current settings
  settings <- dbGetQuery(con, "SELECT * FROM duckdb_settings()")
  
  # Get table statistics
  tables <- dbGetQuery(con, "
    SELECT 
      schema_name,
      table_name,
      estimated_size,
      column_count,
      row_count
    FROM duckdb_tables()
  ")
  
  # Get current queries (if any running)
  queries <- tryCatch(
    dbGetQuery(con, "SELECT * FROM duckdb_queries()"),
    error = function(e) data.frame()
  )
  
  list(
    settings = settings,
    tables = tables,
    active_queries = queries,
    timestamp = Sys.time()
  )
}

# ============================================================================
# Section 5: Resource Management
# ============================================================================

#' Handle memory pressure
#' 
#' @param con DuckDB connection
#' @return List with memory usage information
#' @export
handle_memory_pressure <- function(con) {
  # Check current memory usage
  memory_info <- dbGetQuery(con, "SELECT * FROM duckdb_memory()")
  memory_used <- sum(memory_info$used_memory)
  memory_limit <- dbGetQuery(con, "SELECT value FROM duckdb_settings() WHERE name='memory_limit'")$value
  
  # Parse memory limit
  parse_memory <- function(x) {
    if (grepl("GB$", x)) {
      return(as.numeric(gsub("GB", "", x)) * 1024^3)
    } else if (grepl("MB$", x)) {
      return(as.numeric(gsub("MB", "", x)) * 1024^2)
    }
    return(as.numeric(x))
  }
  
  limit_bytes <- parse_memory(memory_limit)
  usage_pct <- memory_used / limit_bytes * 100
  
  if (usage_pct > 90) {
    warning(sprintf("Memory usage critical: %.1f%%", usage_pct))
    
    # Force checkpoint to free memory
    dbExecute(con, "CHECKPOINT")
    
    # Clear caches
    dbExecute(con, "PRAGMA disable_object_cache")
    dbExecute(con, "PRAGMA enable_object_cache")
  }
  
  return(list(
    memory_used = memory_used,
    memory_limit = limit_bytes,
    usage_percentage = usage_pct
  ))
}

#' Manage temporary files
#' 
#' @param con DuckDB connection
#' @param temp_dir Custom temporary directory path
#' @export
manage_temp_files <- function(con, temp_dir = NULL) {
  if (!is.null(temp_dir)) {
    # Set custom temp directory
    dir.create(temp_dir, showWarnings = FALSE, recursive = TRUE)
    dbExecute(con, sprintf("SET temp_directory='%s'", temp_dir))
  }
  
  # Monitor temp file usage
  temp_files <- list.files(
    temp_dir %||% tempdir(), 
    pattern = "duckdb_temp",
    full.names = TRUE
  )
  
  if (length(temp_files) > 0) {
    temp_sizes <- file.info(temp_files)$size
    total_size <- sum(temp_sizes, na.rm = TRUE)
    
    message(sprintf("Temporary files: %d files, %.2f MB total", 
                   length(temp_files), total_size / 1024^2))
    
    # Clean up old temp files (older than 1 hour)
    old_files <- temp_files[
      difftime(Sys.time(), file.info(temp_files)$mtime, units = "hours") > 1
    ]
    
    if (length(old_files) > 0) {
      unlink(old_files)
      message(sprintf("Cleaned up %d old temporary files", length(old_files)))
    }
  }
}

# ============================================================================
# Section 6: Error Handling
# ============================================================================

#' Execute query with comprehensive error handling
#' 
#' @param con DuckDB connection
#' @param query SQL query
#' @param ... Additional arguments to dbGetQuery
#' @return Query result or error
#' @export
safe_query <- function(con, query, ...) {
  tryCatch({
    dbGetQuery(con, query, ...)
  }, error = function(e) {
    if (grepl("database is locked", e$message)) {
      # Database locked - retry after delay
      Sys.sleep(0.5)
      return(safe_query(con, query, ...))
    } else if (grepl("out of memory", e$message)) {
      # Memory error - try to free memory
      gc()
      dbExecute(con, "CHECKPOINT")
      stop("Query failed due to memory constraints. Consider using chunked processing.")
    } else if (grepl("connection", e$message)) {
      # Connection error - attempt reconnection
      stop("Connection lost. Please reconnect and retry.")
    } else {
      # Other errors
      stop(e$message)
    }
  })
}

#' Gracefully shutdown connection
#' 
#' @param con DuckDB connection
#' @return Connection information before shutdown
#' @export
graceful_shutdown <- function(con) {
  tryCatch({
    # Checkpoint to ensure data is written
    dbExecute(con, "CHECKPOINT")
    
    # Get connection info before closing
    info <- dbGetQuery(con, "SELECT * FROM duckdb_databases()")
    
    # Disconnect properly
    dbDisconnect(con, shutdown = TRUE)
    
    message("Database shutdown gracefully")
    return(info)
  }, error = function(e) {
    warning(sprintf("Error during shutdown: %s", e$message))
    # Force disconnect
    try(dbDisconnect(con, shutdown = TRUE), silent = TRUE)
  })
}

# ============================================================================
# Section 7: Specialized Connection Patterns
# ============================================================================

#' Handle concurrent access with read-write locking
#' 
#' @param db_path Database file path
#' @return List of query results from concurrent readers
#' @export
handle_concurrent_access <- function(db_path) {
  # Single writer connection
  writer_con <- dbConnect(
    duckdb::duckdb(),
    dbdir = db_path,
    read_only = FALSE
  )
  
  # Multiple reader connections
  create_reader <- function() {
    dbConnect(
      duckdb::duckdb(),
      dbdir = db_path,
      read_only = TRUE
    )
  }
  
  readers <- lapply(1:5, function(i) create_reader())
  
  # Writer performs updates
  dbExecute(writer_con, "UPDATE table SET value = value + 1")
  
  # Readers can query simultaneously
  results <- lapply(readers, function(con) {
    dbGetQuery(con, "SELECT * FROM table")
  })
  
  # Clean up
  dbDisconnect(writer_con, shutdown = TRUE)
  lapply(readers, dbDisconnect)
  
  results
}

#' ETL pipeline connection management
#' 
#' @param source_path Source database path
#' @param target_path Target database path
#' @export
run_etl_pipeline <- function(source_path, target_path) {
  # Source connection (read-only)
  source_con <- dbConnect(duckdb::duckdb(), 
                         dbdir = source_path, 
                         read_only = TRUE)
  on.exit(dbDisconnect(source_con), add = TRUE)
  
  # Target connection (read-write)
  target_con <- dbConnect(duckdb::duckdb(), 
                         dbdir = target_path, 
                         read_only = FALSE)
  on.exit(dbDisconnect(target_con, shutdown = TRUE), add = TRUE)
  
  # Configure for ETL workload
  dbExecute(target_con, "SET memory_limit='16GB'")
  dbExecute(target_con, "SET preserve_insertion_order=false")
  
  # Transfer data
  tables <- dbListTables(source_con)
  
  for (table in tables) {
    data <- dbReadTable(source_con, table)
    # Transform data...
    dbWriteTable(target_con, table, data, overwrite = TRUE)
  }
  
  # Checkpoint to ensure data is written
  dbExecute(target_con, "CHECKPOINT")
}

#' Batch processing with connection management
#' 
#' @param db_path Database file path
#' @param batch_size Number of rows per batch
#' @export
batch_process <- function(db_path, batch_size = 10000) {
  con <- dbConnect(duckdb::duckdb(), dbdir = db_path)
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
  
  # Configure for batch processing
  dbExecute(con, "SET memory_limit='4GB'")
  dbExecute(con, "SET checkpoint_threshold='256MB'")
  
  # Process in batches
  total_rows <- dbGetQuery(con, "SELECT COUNT(*) as n FROM source_table")$n
  
  for (offset in seq(0, total_rows, by = batch_size)) {
    batch <- dbGetQuery(con, sprintf(
      "SELECT * FROM source_table LIMIT %d OFFSET %d",
      batch_size, offset
    ))
    
    # Process batch...
    
    # Periodic checkpoint
    if (offset %% (batch_size * 10) == 0) {
      dbExecute(con, "CHECKPOINT")
    }
  }
}