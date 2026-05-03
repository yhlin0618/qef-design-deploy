#' Cached Data Access Functions for Performance Optimization
#'
#' @description
#' Provides memoized data access functions that cache query results to reduce
#' database load and improve dashboard load times. Uses the cachem package for
#' disk-based caching with automatic expiration.
#'
#' @principles
#' - P77: Performance Optimization
#' - R116: Enhanced Data Access with tbl2
#' - MP029: No Fake Data (uses real database queries)
#'
#' @created 2026-01-26
#' @author Claude Code

# Check and load required packages
if (!requireNamespace("memoise", quietly = TRUE)) {
  warning("Package 'memoise' is recommended for caching. Install with: install.packages('memoise')")
}

if (!requireNamespace("cachem", quietly = TRUE)) {
  warning("Package 'cachem' is recommended for caching. Install with: install.packages('cachem')")
}

# -----------------------------------------------------------------------------
# Cache Configuration
# -----------------------------------------------------------------------------

#' Get or create the cache object
#'
#' @param cache_dir Character. Directory for cache storage.
#' @param max_age Numeric. Maximum age of cached items in seconds (default: 6 hours).
#' @return A cachem cache object.
#' @keywords internal
get_app_cache <- function(cache_dir = NULL, max_age = 6 * 60 * 60) {
  if (is.null(cache_dir)) {
    # PERFORMANCE FIX (2026-01-26): Use persistent cache directory instead of tempdir()
    # tempdir() is unique per R session, causing cache misses on Posit Connect
    # where each user request may spawn a new session.
    # Using a persistent directory allows cross-session cache sharing.
    cache_dir <- file.path("cache", "mamba_cache")

    # Fallback to tempdir() if we can't write to the persistent directory
    # (e.g., in restricted environments)
    tryCatch({
      if (!dir.exists(dirname(cache_dir))) {
        dir.create(dirname(cache_dir), recursive = TRUE, showWarnings = FALSE)
      }
      # Test write access
      test_file <- file.path(dirname(cache_dir), ".write_test")
      writeLines("test", test_file)
      unlink(test_file)
    }, error = function(e) {
      # Fallback to tempdir if persistent path is not writable
      cache_dir <<- file.path(tempdir(), "mamba_cache")
      warning("Using tempdir() for cache as persistent directory is not writable: ", e$message)
    })
  }

  if (!dir.exists(cache_dir)) {
    dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  }

  if (requireNamespace("cachem", quietly = TRUE)) {
    cachem::cache_disk(
      dir = cache_dir,
      max_age = max_age,
      max_size = 500 * 1024^2  # 500 MB max
    )
  } else {
    # Fallback to in-memory cache
    new.env(parent = emptyenv())
  }
}

# Create global cache instance
.app_cache <- get_app_cache()

# Cache statistics tracker
.cache_stats <- new.env(parent = emptyenv())
.cache_stats$hits <- list()
.cache_stats$misses <- list()

table_has_column <- function(conn, table_name, column_name) {
  tryCatch({
    column_name %in% DBI::dbListFields(conn, table_name)
  }, error = function(e) {
    FALSE
  })
}

increment_cache_stat <- function(name, hit = TRUE) {
  if (is.null(name) || name == "") return(invisible(NULL))
  bucket <- if (hit) "hits" else "misses"
  current <- .cache_stats[[bucket]][[name]]
  if (is.null(current)) current <- 0L
  .cache_stats[[bucket]][[name]] <- current + 1L

  # Optional logging in production
  if (isTRUE(Sys.getenv("MAMBA_CACHE_LOG_ENABLED", "FALSE") == "TRUE")) {
    log_dir <- Sys.getenv("MAMBA_CACHE_LOG_DIR", "logs")
    if (!dir.exists(log_dir)) {
      dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)
    }
    max_days <- suppressWarnings(as.integer(Sys.getenv("MAMBA_CACHE_LOG_MAX_DAYS", "7")))
    if (is.na(max_days) || max_days < 1) max_days <- 7L

    rotate_cache_logs <- function(dir_path, keep_days) {
      log_files <- list.files(dir_path, pattern = "^cache_hits_\\d{8}\\.log$", full.names = TRUE)
      if (length(log_files) == 0) return(invisible(NULL))

      today <- Sys.Date()
      for (path in log_files) {
        stamp <- sub("^cache_hits_(\\d{8})\\.log$", "\\1", basename(path))
        log_date <- suppressWarnings(as.Date(stamp, format = "%Y%m%d"))
        if (!is.na(log_date) && (today - log_date) > keep_days) {
          try(unlink(path), silent = TRUE)
        }
      }
    }

    rotate_cache_logs(log_dir, max_days)

    log_date <- format(Sys.Date(), "%Y%m%d")
    log_path <- file.path(log_dir, paste0("cache_hits_", log_date, ".log"))
    ts <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
    status <- if (hit) "HIT" else "MISS"
    msg <- sprintf("%s\t%s\t%s\n", ts, name, status)
    cat(msg, file = log_path, append = TRUE)
  }
}

wrap_cache_with_stats <- function(cache, name) {
  if (!is.list(cache) || is.null(cache$get) || is.null(cache$set)) {
    return(cache)
  }

  list(
    get = function(key) {
      value <- cache$get(key)
      if (inherits(value, "key_miss")) {
        increment_cache_stat(name, hit = FALSE)
      } else {
        increment_cache_stat(name, hit = TRUE)
      }
      value
    },
    set = function(key, value) cache$set(key, value),
    reset = function() {
      if (!is.null(cache$reset)) cache$reset()
    },
    keys = function() {
      if (!is.null(cache$keys)) cache$keys() else character(0)
    },
    info = function() {
      if (!is.null(cache$info)) cache$info() else list()
    }
  )
}

#' Clear the application cache
#'
#' @export
clear_app_cache <- function() {
  if (inherits(.app_cache, "cache_disk")) {
    .app_cache$reset()
  } else if (is.environment(.app_cache)) {
    rm(list = ls(.app_cache), envir = .app_cache)
  }
  .cache_stats$hits <- list()
  .cache_stats$misses <- list()
  message("Application cache cleared")
}

# -----------------------------------------------------------------------------
# DNA Pre-computed Data Access (D01_07 output)
# -----------------------------------------------------------------------------
# These functions read from tables pre-computed by D01_07 at ETL/DRV time.
# This provides near-instant (<100ms) data access compared to SQL-level
# aggregation (~500ms) or R-level computation (~2-5s).
#
# PRINCIPLE: MP055 (ALL Category Special Treatment)
# PRINCIPLE: MP064 (ETL-Derivation Separation)
# -----------------------------------------------------------------------------

#' Load pre-computed ECDF data from df_dna_plot_data
#'
#' @description
#' Reads pre-computed ECDF curves from D01_07 output.
#' Falls back to SQL-level computation if pre-computed data unavailable.
#'
#' PERFORMANCE IMPACT:
#' - Pre-computed: ~50ms (read ~2000 rows)
#' - SQL-computed: ~500ms (CUME_DIST on 124k rows)
#' - R-computed: ~2-5s (load + ecdf())
#'
#' @param conn Database connection object.
#' @param platform_id Character. Platform filter ("all", "cbz", etc.).
#' @param metric Character. The metric ("m_value", "r_value", "f_value", "ipt_mean").
#' @param product_line_id Character. Product line filter (default "all").
#' @return data.frame with x (values) and y (cumulative percentages).
#' @export
load_precomputed_ecdf <- function(conn, platform_id = "all", metric = "m_value",
                                   product_line_id = "all") {
  if (is.null(conn)) {
    return(data.frame(x = numeric(), y = numeric()))
  }

  # Check if pre-computed table exists
  has_precomputed <- tryCatch(
    DBI::dbExistsTable(conn, "df_dna_plot_data"),
    error = function(e) FALSE
  )

  if (!has_precomputed) {
    # Fall back to SQL-level computation
    return(load_ecdf_from_sql(conn, platform_id, metric, max_points = 2000, product_line_id = product_line_id))
  }

  # Query pre-computed data
  plat <- platform_id %||% "all"
  pl_id <- product_line_id %||% "all"

  sql <- sprintf("
    SELECT x_value AS x, y_value AS y, total_count
    FROM df_dna_plot_data
    WHERE platform_id = %s
      AND product_line_id = %s
      AND metric = %s
      AND chart_type = 'ecdf'
    ORDER BY row_order
  ", DBI::dbQuoteString(conn, plat),
     DBI::dbQuoteString(conn, pl_id),
     DBI::dbQuoteString(conn, metric))

  result <- tryCatch(
    DBI::dbGetQuery(conn, sql),
    error = function(e) {
      warning("Pre-computed ECDF query failed, falling back to SQL: ", e$message)
      NULL
    }
  )

  if (is.null(result) || nrow(result) == 0) {
    # Fall back to SQL-level computation
    return(load_ecdf_from_sql(conn, platform_id, metric, max_points = 2000, product_line_id = product_line_id))
  }

  # Return only x and y columns
  data.frame(x = result$x, y = result$y)
}

#' Load pre-computed category counts from df_dna_category_counts
#'
#' @description
#' Reads pre-computed category counts from D01_07 output.
#'
#' @param conn Database connection object.
#' @param platform_id Character. Platform filter.
#' @param category_field Character. The category field ("nes_status", "f_value").
#' @param product_line_id Character. Product line filter (default "all").
#' @return data.frame with category, count, and percentage columns.
#' @export
load_precomputed_category_counts <- function(conn, platform_id = "all",
                                              category_field = "nes_status",
                                              product_line_id = "all") {
  if (is.null(conn)) {
    return(data.frame(category = character(), count = integer(), percentage = numeric()))
  }

  has_precomputed <- tryCatch(
    DBI::dbExistsTable(conn, "df_dna_category_counts"),
    error = function(e) FALSE
  )

  if (!has_precomputed) {
    return(load_dna_category_counts(conn, platform_id, category_field, product_line_id = product_line_id))
  }

  plat <- platform_id %||% "all"
  pl_id <- product_line_id %||% "all"

  sql <- sprintf("
    SELECT category_value AS category, count, percentage
    FROM df_dna_category_counts
    WHERE platform_id = %s
      AND product_line_id = %s
      AND category_field = %s
  ", DBI::dbQuoteString(conn, plat),
     DBI::dbQuoteString(conn, pl_id),
     DBI::dbQuoteString(conn, category_field))

  result <- tryCatch(
    DBI::dbGetQuery(conn, sql),
    error = function(e) {
      warning("Pre-computed category counts failed, falling back: ", e$message)
      NULL
    }
  )

  if (is.null(result) || nrow(result) == 0) {
    return(load_dna_category_counts(conn, platform_id, category_field, product_line_id = product_line_id))
  }

  result
}

#' Load pre-computed summary statistics from df_dna_summary_stats
#'
#' @description
#' Reads pre-computed summary statistics from D01_07 output.
#'
#' @param conn Database connection object.
#' @param platform_id Character. Platform filter.
#' @param metric Character. The metric.
#' @param product_line_id Character. Product line filter (default "all").
#' @return list with n, mean, median, sd, min, max, q1, q3.
#' @export
load_precomputed_summary_stats <- function(conn, platform_id = "all",
                                            metric = "m_value",
                                            product_line_id = "all") {
  if (is.null(conn)) {
    return(list(n = 0))
  }

  has_precomputed <- tryCatch(
    DBI::dbExistsTable(conn, "df_dna_summary_stats"),
    error = function(e) FALSE
  )

  if (!has_precomputed) {
    return(load_dna_summary_stats(conn, platform_id, metric, product_line_id = product_line_id))
  }

  plat <- platform_id %||% "all"
  pl_id <- product_line_id %||% "all"

  sql <- sprintf("
    SELECT n, mean_val, median_val, sd_val, min_val, max_val, q1_val, q3_val
    FROM df_dna_summary_stats
    WHERE platform_id = %s
      AND product_line_id = %s
      AND metric = %s
  ", DBI::dbQuoteString(conn, plat),
     DBI::dbQuoteString(conn, pl_id),
     DBI::dbQuoteString(conn, metric))

  result <- tryCatch(
    DBI::dbGetQuery(conn, sql),
    error = function(e) {
      warning("Pre-computed summary stats failed, falling back: ", e$message)
      NULL
    }
  )

  if (is.null(result) || nrow(result) == 0) {
    return(load_dna_summary_stats(conn, platform_id, metric, product_line_id = product_line_id))
  }

  list(
    n = as.integer(result$n[1]),
    mean = as.numeric(result$mean_val[1]),
    median = as.numeric(result$median_val[1]),
    sd = as.numeric(result$sd_val[1]),
    min = as.numeric(result$min_val[1]),
    max = as.numeric(result$max_val[1]),
    q1 = as.numeric(result$q1_val[1]),
    q3 = as.numeric(result$q3_val[1])
  )
}

# Note: Memoised versions of pre-computed functions are defined later in the file
# after create_memoised() is available.

# -----------------------------------------------------------------------------
# DNA Distribution Aggregation (for microDNADistribution) - SQL-level fallback
# -----------------------------------------------------------------------------

#' Load DNA distribution summary (aggregated at SQL level)
#'
#' @description
#' Returns pre-aggregated DNA distribution data for visualization.
#' Instead of loading all 124k+ records, this returns only the aggregated
#' statistics needed for ECDF plots and histograms.
#'
#' @param conn Database connection object.
#' @param platform_id Character. Platform filter ("all", "amz", "eby", etc.).
#' @param metric Character. The metric to aggregate ("m_value", "r_value", "f_value", "ipt_mean").
#' @param product_line_id Character. Product line filter (default "all").
#' @return A tibble with aggregated distribution data.
#' @export
load_dna_distribution_summary <- function(conn, platform_id = "all", metric = "m_value",
                                          product_line_id = "all") {
  req_conn <- !is.null(conn)
  if (!req_conn) {
    warning("No database connection provided")
    return(tibble::tibble())
  }

  # Build the aggregation query at SQL level
  tbl_ref <- tbl2(conn, "df_dna_by_customer")

  # Apply platform filter
  if (!is.null(platform_id) && !is.na(platform_id) && platform_id != "all") {
    tbl_ref <- dplyr::filter(tbl_ref, platform_id == !!platform_id)
  }

  # Apply product_line filter if column exists
  has_pl <- table_has_column(conn, "df_dna_by_customer", "product_line_id_filter")
  if (has_pl && !is.null(product_line_id) && !is.na(product_line_id) && product_line_id != "all") {
    tbl_ref <- dplyr::filter(tbl_ref, product_line_id_filter == !!product_line_id)
  }

  # Select only the needed metric column
  valid_metrics <- c("m_value", "r_value", "f_value", "ipt_mean", "nes_status")
  if (!metric %in% valid_metrics) {
    warning("Invalid metric: ", metric, ". Using m_value.")
    metric <- "m_value"
  }

  # For distribution plots, we need the raw values but only the specific column
  # This reduces data transfer significantly
  result <- tbl_ref %>%
    dplyr::select(dplyr::all_of(c("customer_id", metric))) %>%
    dplyr::collect()

  return(result)
}

#' Load ECDF data computed at SQL level (PERFORMANCE OPTIMIZATION)
#'
#' @description
#' Computes ECDF directly in the database using window functions, returning
#' only the unique values and their cumulative distribution percentages.
#' This dramatically reduces data transfer compared to loading all raw values.
#'
#' PERFORMANCE IMPACT (2026-01-26):
#' - Before: Load 124k raw values (~2MB), compute ECDF in R
#' - After: Compute in SQL, return ~5,000 unique values (~100KB)
#' - Expected speedup: 10-20x for large datasets
#'
#' @param conn Database connection object.
#' @param platform_id Character. Platform filter ("all", "amz", "eby", etc.).
#' @param metric Character. The metric to compute ECDF for.
#' @param max_points Integer. Maximum points to return (for downsampling). Default 2000.
#' @param product_line_id Character. Product line filter (default "all").
#' @return A data.frame with columns x (values) and y (cumulative percentages).
#' @export
load_ecdf_from_sql <- function(conn, platform_id = "all", metric = "m_value",
                               max_points = 2000, product_line_id = "all") {
  req_conn <- !is.null(conn)
  if (!req_conn) {
    warning("No database connection provided")
    return(data.frame(x = numeric(), y = numeric()))
  }

  valid_metrics <- c("m_value", "r_value", "f_value", "ipt_mean")
  if (!metric %in% valid_metrics) {
    warning("Invalid metric: ", metric, ". Using m_value.")
    metric <- "m_value"
  }

  # Detect database type
  db_info <- tryCatch(DBI::dbGetInfo(conn), error = function(e) list())
  dbms_name <- tolower(db_info$dbms.name %||% "")
  is_duckdb <- grepl("duckdb", dbms_name)
  is_postgres <- grepl("postgres", dbms_name)

  metric_id <- DBI::dbQuoteIdentifier(conn, metric)
  metric_sql <- as.character(metric_id)

  # Build WHERE clause (platform + product_line + non-null metric)
  where_clauses <- c(sprintf("%s IS NOT NULL", metric_sql))
  if (!is.null(platform_id) && !is.na(platform_id) && platform_id != "all") {
    where_clauses <- c(where_clauses, paste0("platform_id = ", DBI::dbQuoteString(conn, platform_id)))
  }

  has_pl <- table_has_column(conn, "df_dna_by_customer", "product_line_id_filter")
  if (has_pl && !is.null(product_line_id) && !is.na(product_line_id) && product_line_id != "all") {
    where_clauses <- c(where_clauses, paste0("product_line_id_filter = ", DBI::dbQuoteString(conn, product_line_id)))
  }

  where_sql <- paste(where_clauses, collapse = " AND ")

  # Use CUME_DIST() window function to compute ECDF at SQL level
  # Both DuckDB and PostgreSQL support this syntax
  if (is_duckdb || is_postgres) {
    sql <- sprintf("
      WITH ranked AS (
        SELECT DISTINCT %s AS x,
               CUME_DIST() OVER (ORDER BY %s) AS y
        FROM df_dna_by_customer
        WHERE %s
      )
      SELECT x, y FROM ranked
      ORDER BY x
    ", metric_sql, metric_sql, metric_sql, where_sql)

    result <- tryCatch(
      DBI::dbGetQuery(conn, sql),
      error = function(e) {
        warning("SQL ECDF computation failed, falling back to R: ", e$message)
        NULL
      }
    )

    if (!is.null(result) && nrow(result) > 0) {
      # Downsample using quantiles if too many unique values
      if (nrow(result) > max_points) {
        target_y <- seq(0, 1, length.out = max_points)
        interp <- stats::approx(result$y, result$x, xout = target_y, ties = "ordered", rule = 2)
        result <- data.frame(x = interp$y, y = interp$x)
      }
      return(result)
    }
  }

  # Fallback: load data and compute in R
  tbl_ref <- tbl2(conn, "df_dna_by_customer")
  if (!is.null(platform_id) && !is.na(platform_id) && platform_id != "all") {
    tbl_ref <- dplyr::filter(tbl_ref, platform_id == !!platform_id)
  }
  if (has_pl && !is.null(product_line_id) && !is.na(product_line_id) && product_line_id != "all") {
    tbl_ref <- dplyr::filter(tbl_ref, product_line_id_filter == !!product_line_id)
  }

  dat <- tbl_ref %>%
    dplyr::select(dplyr::all_of(metric)) %>%
    dplyr::collect()

  v <- dat[[metric]]
  v <- v[!is.na(v)]
  if (length(v) == 0) {
    return(data.frame(x = numeric(), y = numeric()))
  }

  fn <- stats::ecdf(v)
  x <- sort(unique(v))

  # Downsample if needed
  if (length(x) > max_points) {
    x <- unique(stats::quantile(v, probs = seq(0, 1, length.out = max_points), na.rm = TRUE))
  }

  data.frame(x = x, y = fn(x))
}

#' Load DNA category distribution (pre-aggregated counts)
#'
#' @description
#' Returns category counts for bar charts, computed at the database level.
#'
#' @param conn Database connection object.
#' @param platform_id Character. Platform filter.
#' @param category_field Character. The field to aggregate (e.g., "nes_status", "f_value").
#' @param product_line_id Character. Product line filter (default "all").
#' @return A tibble with category and count columns.
#' @export
load_dna_category_counts <- function(conn, platform_id = "all", category_field = "nes_status",
                                     product_line_id = "all") {
  req_conn <- !is.null(conn)
  if (!req_conn) {
    warning("No database connection provided")
    return(tibble::tibble(category = character(), count = integer()))
  }

  tbl_ref <- tbl2(conn, "df_dna_by_customer")

  # Apply platform filter
  if (!is.null(platform_id) && !is.na(platform_id) && platform_id != "all") {
    tbl_ref <- dplyr::filter(tbl_ref, platform_id == !!platform_id)
  }

  has_pl <- table_has_column(conn, "df_dna_by_customer", "product_line_id_filter")
  if (has_pl && !is.null(product_line_id) && !is.na(product_line_id) && product_line_id != "all") {
    tbl_ref <- dplyr::filter(tbl_ref, product_line_id_filter == !!product_line_id)
  }

  # Aggregate at SQL level - this is the key optimization
  result <- tbl_ref %>%
    dplyr::group_by(!!rlang::sym(category_field)) %>%
    dplyr::summarise(
      count = dplyr::n(),
      .groups = "drop"
    ) %>%
    dplyr::collect() %>%
    dplyr::rename(category = !!category_field)

  return(result)
}

#' Load DNA summary statistics (computed at SQL level)
#'
#' @description
#' Returns summary statistics (mean, median, min, max, etc.) computed directly
#' in the database, avoiding the need to transfer raw data.
#'
#' @param conn Database connection object.
#' @param platform_id Character. Platform filter.
#' @param metric Character. The metric to summarize.
#' @param product_line_id Character. Product line filter (default "all").
#' @return A list with summary statistics.
#' @export
load_dna_summary_stats <- function(conn, platform_id = "all", metric = "m_value",
                                   product_line_id = "all") {
  req_conn <- !is.null(conn)
  if (!req_conn) {
    return(list(n = 0))
  }

  valid_metrics <- c("m_value", "r_value", "f_value", "ipt_mean")
  if (!metric %in% valid_metrics) {
    warning("Invalid metric for summary stats: ", metric)
    return(list(n = 0))
  }

  db_info <- tryCatch(DBI::dbGetInfo(conn), error = function(e) list())
  dbms_name <- tolower(db_info$dbms.name %||% "")
  is_duckdb <- grepl("duckdb", dbms_name)
  is_postgres <- grepl("postgres", dbms_name)

  metric_id <- DBI::dbQuoteIdentifier(conn, metric)
  metric_sql <- as.character(metric_id)

  where_clauses <- character(0)
  if (!is.null(platform_id) && !is.na(platform_id) && platform_id != "all") {
    where_clauses <- c(where_clauses, paste0("platform_id = ", DBI::dbQuoteString(conn, platform_id)))
  }

  has_pl <- table_has_column(conn, "df_dna_by_customer", "product_line_id_filter")
  if (has_pl && !is.null(product_line_id) && !is.na(product_line_id) && product_line_id != "all") {
    where_clauses <- c(where_clauses, paste0("product_line_id_filter = ", DBI::dbQuoteString(conn, product_line_id)))
  }

  where_sql <- if (length(where_clauses) > 0) {
    paste0(" WHERE ", paste(where_clauses, collapse = " AND "))
  } else {
    ""
  }

  if (is_postgres) {
    sql <- sprintf(
      "SELECT COUNT(*) AS n,
              AVG(%s) AS mean_val,
              MIN(%s) AS min_val,
              MAX(%s) AS max_val,
              STDDEV_SAMP(%s) AS sd_val,
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY %s) AS median_val,
              PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY %s) AS q1_val,
              PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY %s) AS q3_val
       FROM df_dna_by_customer%s",
      metric_sql, metric_sql, metric_sql, metric_sql,
      metric_sql, metric_sql, metric_sql, where_sql
    )
  } else if (is_duckdb) {
    sql <- sprintf(
      "SELECT COUNT(*) AS n,
              AVG(%s) AS mean_val,
              MIN(%s) AS min_val,
              MAX(%s) AS max_val,
              STDDEV_SAMP(%s) AS sd_val,
              MEDIAN(%s) AS median_val,
              QUANTILE_CONT(%s, 0.25) AS q1_val,
              QUANTILE_CONT(%s, 0.75) AS q3_val
       FROM df_dna_by_customer%s",
      metric_sql, metric_sql, metric_sql, metric_sql,
      metric_sql, metric_sql, metric_sql, where_sql
    )
  } else {
    # Fallback to dbplyr summarise for other DBs
    tbl_ref <- tbl2(conn, "df_dna_by_customer")

    if (!is.null(platform_id) && !is.na(platform_id) && platform_id != "all") {
      tbl_ref <- dplyr::filter(tbl_ref, platform_id == !!platform_id)
    }
    if (has_pl && !is.null(product_line_id) && !is.na(product_line_id) && product_line_id != "all") {
      tbl_ref <- dplyr::filter(tbl_ref, product_line_id_filter == !!product_line_id)
    }

    stats <- tbl_ref %>%
      dplyr::summarise(
        n = dplyr::n(),
        mean_val = mean(!!rlang::sym(metric), na.rm = TRUE),
        min_val = min(!!rlang::sym(metric), na.rm = TRUE),
        max_val = max(!!rlang::sym(metric), na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::collect()

    return(list(
      n = as.integer(stats$n[1]),
      mean = as.numeric(stats$mean_val[1]),
      min = as.numeric(stats$min_val[1]),
      max = as.numeric(stats$max_val[1])
    ))
  }

  stats <- tryCatch(DBI::dbGetQuery(conn, sql), error = function(e) NULL)
  if (is.null(stats) || nrow(stats) == 0) {
    return(list(n = 0))
  }

  list(
    n = as.integer(stats$n[1]),
    mean = as.numeric(stats$mean_val[1]),
    median = as.numeric(stats$median_val[1]),
    sd = as.numeric(stats$sd_val[1]),
    min = as.numeric(stats$min_val[1]),
    max = as.numeric(stats$max_val[1]),
    q1 = as.numeric(stats$q1_val[1]),
    q3 = as.numeric(stats$q3_val[1])
  )
}

# -----------------------------------------------------------------------------
# Customer Data Caching (for microCustomer)
# -----------------------------------------------------------------------------

#' Load customer dropdown options (limited to top N)
#'
#' @description
#' Returns a limited set of customer options for dropdown menus,
#' avoiding the need to load all customer records.
#'
#' @param conn Database connection object.
#' @param platform_id Character. Platform filter.
#' @param product_line_id_sliced Character. Internal product-line filtered state
#'   used for pre-filtered derivation outputs (default "all").
#' @param limit Integer. Maximum number of customers to return.
#' @return A tibble with customer_id, buyer_name, and email.
#' @export
load_customer_dropdown_options <- function(conn, platform_id = "all",
                                           product_line_id_sliced = "all",
                                           limit = 100) {
  req_conn <- !is.null(conn)
  if (!req_conn) {
    return(tibble::tibble(customer_id = integer(), buyer_name = character(), email = character()))
  }

  has_pl <- table_has_column(conn, "df_dna_by_customer", "product_line_id_filter")
  if (!has_pl &&
      !is.null(product_line_id_sliced) &&
      !is.na(product_line_id_sliced) &&
      product_line_id_sliced != "all") {
    return(tibble::tibble(customer_id = integer(), buyer_name = character(), email = character()))
  }

  # Get profile data with limit
  prof_tbl <- tbl2(conn, "df_profile_by_customer") %>%
    dplyr::select(customer_id, platform_id, buyer_name, email)

  dna_cols <- c("customer_id", "platform_id")
  if (has_pl) {
    dna_cols <- c(dna_cols, "product_line_id_filter")
  }

  dna_tbl <- tbl2(conn, "df_dna_by_customer") %>%
    dplyr::select(dplyr::all_of(dna_cols))

  if (!is.null(platform_id) && !is.na(platform_id) && platform_id != "all") {
    prof_tbl <- dplyr::filter(prof_tbl, platform_id == !!platform_id)
    dna_tbl <- dplyr::filter(dna_tbl, platform_id == !!platform_id)
  }

  if (has_pl &&
      !is.null(product_line_id_sliced) &&
      !is.na(product_line_id_sliced) &&
      product_line_id_sliced != "all") {
    local_pl <- as.character(product_line_id_sliced)
    dna_tbl <- dplyr::filter(dna_tbl, product_line_id_filter == !!local_pl)
  }

  # Only select needed columns and limit rows
  # Use head() instead of slice_head() for PostgreSQL/Supabase compatibility
  result <- prof_tbl %>%
    dplyr::semi_join(dna_tbl, by = c("customer_id", "platform_id")) %>%
    head(n = limit) %>%
    dplyr::collect()

  return(result)
}

#' Load single customer data (for detail view)
#'
#' @description
#' Fetches data for a single customer, optimized for the detail view.
#' Uses SQL-level joins instead of loading entire tables.
#'
#' @param conn Database connection object.
#' @param customer_id Integer. The customer ID to load.
#' @return A tibble with combined DNA and profile data for the customer.
#' @export
load_customer_detail <- function(conn, customer_id) {
  req_conn <- !is.null(conn) && !is.null(customer_id) && !is.na(customer_id)
  if (!req_conn) {
    return(NULL)
  }

  cust_id <- as.integer(customer_id)

  # Get DNA data for single customer
  dna <- tbl2(conn, "df_dna_by_customer") %>%
    dplyr::filter(customer_id == !!cust_id) %>%
    dplyr::collect()

  if (nrow(dna) == 0) {
    return(NULL)
  }

  # Get profile data for single customer
 prof <- tbl2(conn, "df_profile_by_customer") %>%
    dplyr::filter(customer_id == !!cust_id) %>%
    dplyr::collect()

  if (nrow(prof) == 0) {
    return(dna)  # Return DNA data even without profile
  }

  # Join the two
  result <- dplyr::left_join(dna, prof, by = "customer_id", suffix = c("_dna", "_prof"))

  return(result)
}

# -----------------------------------------------------------------------------
# Memoized Versions (with caching)
# -----------------------------------------------------------------------------

#' Create memoized version of a function
#'
#' @param fn Function to memoize.
#' @param cache Cache object to use.
#' @return Memoized function.
#' @keywords internal
create_memoised <- function(fn, cache = .app_cache, name = NULL) {
  if (requireNamespace("memoise", quietly = TRUE)) {
    cache_used <- wrap_cache_with_stats(cache, name)
    memoise::memoise(fn, cache = cache_used)
  } else {
    # Return original function if memoise not available
    fn
  }
}

# Create memoized versions
load_dna_distribution_summary_cached <- create_memoised(load_dna_distribution_summary, name = "load_dna_distribution_summary")
load_dna_category_counts_cached <- create_memoised(load_dna_category_counts, name = "load_dna_category_counts")
load_dna_summary_stats_cached <- create_memoised(load_dna_summary_stats, name = "load_dna_summary_stats")
load_customer_dropdown_options_cached <- create_memoised(load_customer_dropdown_options, name = "load_customer_dropdown_options")
load_ecdf_from_sql_cached <- create_memoised(load_ecdf_from_sql, name = "load_ecdf_from_sql")

# Pre-computed data access (D01_07 output) - prioritized over SQL-level computation
load_precomputed_ecdf_cached <- create_memoised(load_precomputed_ecdf, name = "load_precomputed_ecdf")
load_precomputed_category_counts_cached <- create_memoised(load_precomputed_category_counts, name = "load_precomputed_category_counts")
load_precomputed_summary_stats_cached <- create_memoised(load_precomputed_summary_stats, name = "load_precomputed_summary_stats")

# Note: load_customer_detail is NOT memoized because customer IDs are unique
# and caching individual customer data would consume too much memory

# -----------------------------------------------------------------------------
# Utility Functions
# -----------------------------------------------------------------------------

#' Check if caching is available
#'
#' @return Logical indicating if memoise is available.
#' @export
is_caching_available <- function() {
  requireNamespace("memoise", quietly = TRUE) &&
    requireNamespace("cachem", quietly = TRUE)
}

#' Get cache statistics
#'
#' @return List with cache information.
#' @export
get_cache_info <- function() {
  if (inherits(.app_cache, "cache_disk")) {
    list(
      type = "disk",
      dir = .app_cache$info()$dir,
      size = .app_cache$info()$current_size,
      max_size = .app_cache$info()$max_size
    )
  } else {
    list(
      type = "memory",
      items = length(ls(.app_cache))
    )
  }
}

#' Get cache hit/miss statistics
#'
#' @return data.frame with hits, misses, and hit_rate per function.
#' @export
get_cache_stats <- function() {
  names_all <- unique(c(names(.cache_stats$hits), names(.cache_stats$misses)))
  if (length(names_all) == 0) {
    return(data.frame())
  }

  data.frame(
    function_name = names_all,
    hits = vapply(names_all, function(n) .cache_stats$hits[[n]] %||% 0L, integer(1)),
    misses = vapply(names_all, function(n) .cache_stats$misses[[n]] %||% 0L, integer(1)),
    stringsAsFactors = FALSE
  ) %>%
    dplyr::mutate(
      total = hits + misses,
      hit_rate = ifelse(total > 0, round(hits / total, 3), NA_real_)
    )
}
