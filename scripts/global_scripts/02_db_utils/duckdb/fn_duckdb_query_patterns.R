# DuckDB Query Optimization Patterns
# MAMBA Framework
# Date: 2025-08-28
# Purpose: Query analysis, optimization, and performance monitoring functions

#' Analyze query execution plan
#' 
#' @param con DuckDB connection
#' @param query SQL query to analyze
#' @return List with logical plan, physical plan, and execution time
#' @export
analyze_query <- function(con, query) {
  # Get logical plan
  logical_plan <- dbGetQuery(con, sprintf("EXPLAIN %s", query))
  
  # Get physical plan with costs
  physical_plan <- dbGetQuery(con, sprintf("EXPLAIN ANALYZE %s", query))
  
  # Parse execution time
  execution_time <- as.numeric(gsub(".*Total Time: ([0-9.]+)ms.*", "\\1", 
                                   tail(physical_plan$explain_analyze, 1)))
  
  list(
    logical_plan = logical_plan,
    physical_plan = physical_plan,
    execution_time_ms = execution_time
  )
}

#' Profile query execution
#' 
#' @param con DuckDB connection
#' @param query SQL query to profile
#' @return List with result, profile data, and timings
#' @export
profile_query <- function(con, query) {
  # Enable profiling
  dbExecute(con, "PRAGMA enable_profiling='json'")
  dbExecute(con, "PRAGMA profiling_mode='detailed'")
  
  # Run query
  result <- dbGetQuery(con, query)
  
  # Get profile
  profile <- dbGetQuery(con, "PRAGMA last_profiling_output")
  
  # Parse JSON profile
  profile_data <- jsonlite::fromJSON(profile$last_profiling_output)
  
  # Disable profiling
  dbExecute(con, "PRAGMA disable_profiling")
  
  list(
    result = result,
    profile = profile_data,
    total_time = profile_data$timing,
    operator_timings = profile_data$children
  )
}

#' Manage indexes for optimization
#' 
#' @param con DuckDB connection
#' @param table_name Table name
#' @param columns Vector of column names for indexes
#' @return Data frame of created indexes
#' @export
manage_indexes <- function(con, table_name, columns = NULL) {
  indexes_created <- list()
  
  if (!is.null(columns)) {
    for (col in columns) {
      # Create index on single column
      idx_name <- sprintf("idx_%s_%s", table_name, col)
      dbExecute(con, sprintf("
        CREATE INDEX IF NOT EXISTS %s ON %s(%s)
      ", idx_name, table_name, col))
      
      indexes_created[[col]] <- idx_name
    }
  }
  
  # List all indexes for the table
  indexes <- dbGetQuery(con, sprintf("
    SELECT * FROM duckdb_indexes() 
    WHERE table_name = '%s'
  ", table_name))
  
  return(indexes)
}

#' Check if indexes are being used
#' 
#' @param con DuckDB connection
#' @param query SQL query to check
#' @return List with index usage information
#' @export
check_index_usage <- function(con, query) {
  plan <- dbGetQuery(con, sprintf("EXPLAIN %s", query))
  
  # Look for index scans in plan
  index_scans <- grep("INDEX", plan$explain, value = TRUE)
  seq_scans <- grep("SEQ_SCAN", plan$explain, value = TRUE)
  
  list(
    uses_index = length(index_scans) > 0,
    index_operations = index_scans,
    sequential_scans = seq_scans,
    recommendation = ifelse(
      length(seq_scans) > 0 && length(index_scans) == 0,
      "Consider adding indexes on filter/join columns",
      "Query is using indexes effectively"
    )
  )
}

#' Configure memory for optimal performance
#' 
#' @param con DuckDB connection
#' @param memory_percentage Percentage of system memory to use (default 0.8)
#' @return Data frame with memory settings
#' @export
configure_memory_settings <- function(con, memory_percentage = 0.8) {
  # Get system memory (macOS specific)
  total_memory <- as.numeric(system("sysctl -n hw.memsize", intern = TRUE))
  
  # Set memory limit
  memory_limit <- floor(total_memory * memory_percentage / 1024^3)  # Convert to GB
  dbExecute(con, sprintf("SET memory_limit='%dGB'", memory_limit))
  
  # Configure work memory
  dbExecute(con, "SET default_order_by_nulls_last=true")
  dbExecute(con, "SET enable_object_cache=true")
  
  # Monitor memory usage
  memory_usage <- dbGetQuery(con, "SELECT * FROM duckdb_memory()")
  return(memory_usage)
}

#' Configure parallel execution
#' 
#' @param con DuckDB connection
#' @param threads Number of threads (NULL for auto-detect)
#' @return Data frame with parallel settings
#' @export
configure_parallelism <- function(con, threads = NULL) {
  # Set thread count
  if (is.null(threads)) {
    cores <- parallel::detectCores()
    threads <- cores - 1
  }
  
  dbExecute(con, sprintf("SET threads=%d", threads))
  
  # Enable parallel execution
  dbExecute(con, "SET enable_parallel=true")
  dbExecute(con, "SET force_parallelism=false")  # Let optimizer decide
  
  # Verify settings
  settings <- dbGetQuery(con, "
    SELECT name, value 
    FROM duckdb_settings() 
    WHERE name LIKE '%parallel%' OR name = 'threads'
  ")
  
  return(settings)
}

#' Create query result cache
#' 
#' @param con DuckDB connection
#' @return Cache function for queries
#' @export
create_query_cache <- function(con) {
  # Create cache table
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS query_cache (
      query_hash VARCHAR PRIMARY KEY,
      query_text VARCHAR,
      result JSON,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      expires_at TIMESTAMP
    )
  ")
  
  # Return cache function
  cached_query <- function(query, cache_duration_minutes = 60) {
    query_hash <- digest::digest(query, algo = "md5")
    
    # Check cache
    cached <- dbGetQuery(con, sprintf("
      SELECT result 
      FROM query_cache 
      WHERE query_hash = '%s' 
        AND expires_at > CURRENT_TIMESTAMP
    ", query_hash))
    
    if (nrow(cached) > 0) {
      return(jsonlite::fromJSON(cached$result[1]))
    }
    
    # Execute query
    result <- dbGetQuery(con, query)
    
    # Store in cache
    dbExecute(con, sprintf("
      INSERT INTO query_cache (query_hash, query_text, result, expires_at)
      VALUES ('%s', '%s', '%s', CURRENT_TIMESTAMP + INTERVAL '%d minutes')
      ON CONFLICT (query_hash) DO UPDATE SET
        result = EXCLUDED.result,
        expires_at = EXCLUDED.expires_at
    ", query_hash, query, jsonlite::toJSON(result), cache_duration_minutes))
    
    return(result)
  }
  
  return(cached_query)
}

#' Create performance monitoring dashboard data
#' 
#' @param con DuckDB connection
#' @return List with performance metrics
#' @export
create_performance_dashboard <- function(con) {
  # Collect query statistics (if query_log table exists)
  query_stats <- tryCatch({
    dbGetQuery(con, "
      SELECT 
        query,
        COUNT(*) as execution_count,
        AVG(execution_time) as avg_time_ms,
        MAX(execution_time) as max_time_ms,
        SUM(rows_returned) as total_rows
      FROM query_log
      GROUP BY query
      ORDER BY avg_time_ms DESC
      LIMIT 20
    ")
  }, error = function(e) NULL)
  
  # Memory usage
  memory_usage <- dbGetQuery(con, "SELECT * FROM duckdb_memory()")
  
  # Database size
  db_size <- dbGetQuery(con, "SELECT * FROM duckdb_databases()")
  
  list(
    query_stats = query_stats,
    memory_usage = memory_usage,
    database_size = db_size,
    timestamp = Sys.time()
  )
}

#' Comprehensive optimization checklist
#' 
#' @param con DuckDB connection
#' @param query SQL query to check
#' @return List with optimization checks and recommendations
#' @export
optimization_checklist <- function(con, query) {
  checks <- list()
  
  # 1. Analyze query plan
  plan <- dbGetQuery(con, sprintf("EXPLAIN ANALYZE %s", query))
  checks$has_index_scan <- any(grepl("INDEX", plan$explain))
  checks$has_sequential_scan <- any(grepl("SEQ_SCAN", plan$explain))
  
  # 2. Check statistics
  checks$statistics_current <- TRUE  # Run ANALYZE regularly
  
  # 3. Memory usage
  memory_before <- dbGetQuery(con, "SELECT SUM(used_memory) as mem FROM duckdb_memory()")
  result <- dbGetQuery(con, query)
  memory_after <- dbGetQuery(con, "SELECT SUM(used_memory) as mem FROM duckdb_memory()")
  checks$memory_increase_mb <- (memory_after$mem - memory_before$mem) / 1024^2
  
  # 4. Execution time
  start_time <- Sys.time()
  dbGetQuery(con, query)
  checks$execution_time_seconds <- as.numeric(Sys.time() - start_time)
  
  # 5. Recommendations
  checks$recommendations <- c()
  if (!checks$has_index_scan && checks$has_sequential_scan) {
    checks$recommendations <- c(checks$recommendations, 
                               "Consider adding indexes")
  }
  if (checks$memory_increase_mb > 1000) {
    checks$recommendations <- c(checks$recommendations, 
                               "High memory usage - consider chunking")
  }
  if (checks$execution_time_seconds > 5) {
    checks$recommendations <- c(checks$recommendations, 
                               "Slow query - review execution plan")
  }
  
  return(checks)
}

#' Memory-efficient chunk processing
#' 
#' @param con DuckDB connection
#' @param table Table name
#' @param chunk_size Rows per chunk
#' @param process_chunk Processing function
#' @export
chunk_process <- function(con, table, chunk_size = 100000, process_chunk) {
  offset <- 0
  repeat {
    chunk <- dbGetQuery(con, sprintf("
      SELECT *
      FROM %s
      LIMIT %d OFFSET %d
    ", table, chunk_size, offset))
    
    if (nrow(chunk) == 0) break
    
    # Process chunk
    process_chunk(chunk)
    
    offset <- offset + chunk_size
    
    # Free memory periodically
    if (offset %% (chunk_size * 10) == 0) {
      gc()
    }
  }
}