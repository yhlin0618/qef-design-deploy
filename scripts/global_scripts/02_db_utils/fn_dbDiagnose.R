#' Diagnose Database Connection Configuration
#'
#' @description
#' Provides comprehensive diagnostics for both DuckDB and Supabase database backends.
#' Useful for debugging connection issues and verifying configuration before deployment.
#'
#' @param config_path Path to app_config.yaml. Default: "app_config.yaml"
#' @param test_connection Logical. Whether to test actual connections. Default: FALSE
#' @param verbose Logical. Print detailed output. Default: TRUE
#'
#' @return A list with diagnostic information:
#' \itemize{
#'   \item config: Configuration from app_config.yaml
#'   \item duckdb: DuckDB backend status
#'   \item supabase: Supabase backend status
#'   \item recommendation: Suggested action based on current state
#'   \item mode_explanation: What the current mode means
#' }
#'
#' @details
#' Following Principles:
#'   - MP142: Configuration-Driven Pipeline
#'   - DM_R023: Universal DBI Approach
#'   - SO_R007: One Function One File
#'
#' @examples
#' \dontrun{
#' # Basic diagnostics
#' diag <- dbDiagnose()
#'
#' # Test actual connections
#' diag <- dbDiagnose(test_connection = TRUE)
#'
#' # Check specific config file
#' diag <- dbDiagnose(config_path = "custom_config.yaml")
#' }
#'
#' @export
dbDiagnose <- function(
    config_path = "app_config.yaml",
    test_connection = FALSE,
    verbose = TRUE
) {
  # Null coalescing operator
  `%||%` <- function(x, y) if (is.null(x) || (is.character(x) && x == "")) y else x

  result <- list(
    timestamp = Sys.time(),
    config = list(),
    duckdb = list(),
    supabase = list(),
    recommendation = "",
    mode_explanation = ""
  )

  # ─────────────────────────────────────────────────────────────────
  # 1. Load and analyze configuration
  # ─────────────────────────────────────────────────────────────────
  if (verbose) {
    cat("\n")
    cat("=== Database Connection Diagnostics ===\n")
    cat("Timestamp:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n\n")
  }

  if (file.exists(config_path)) {
    tryCatch({
      config <- yaml::read_yaml(config_path)
      db_config <- config$database %||% list(mode = "auto")
      result$config <- list(
        path = config_path,
        found = TRUE,
        mode = db_config$mode %||% "auto",
        duckdb_path = db_config$duckdb$path %||% "data/app_data/app_data.duckdb",
        duckdb_read_only = db_config$duckdb$read_only %||% TRUE
      )
    }, error = function(e) {
      result$config <- list(
        path = config_path,
        found = TRUE,
        parse_error = e$message,
        mode = "auto"
      )
    })
  } else {
    result$config <- list(
      path = config_path,
      found = FALSE,
      mode = "auto"
    )
  }

  if (verbose) {
    cat("[Config] ", config_path, "\n")
    cat("  Found: ", result$config$found, "\n")
    cat("  Mode: ", result$config$mode, "\n")
    if (!is.null(result$config$parse_error)) {
      cat("  Parse Error: ", result$config$parse_error, "\n")
    }
    cat("\n")
  }

  # ─────────────────────────────────────────────────────────────────
  # 2. Check DuckDB availability
  # ─────────────────────────────────────────────────────────────────
  duckdb_path <- result$config$duckdb_path %||% "data/app_data/app_data.duckdb"

  result$duckdb <- list(
    path = duckdb_path,
    exists = file.exists(duckdb_path),
    size = NA,
    size_human = NA,
    valid = FALSE,
    is_lfs_pointer = FALSE,
    can_connect = NA,
    connection_error = NA
  )

  if (result$duckdb$exists) {
    file_size <- file.info(duckdb_path)$size
    result$duckdb$size <- file_size
    result$duckdb$size_human <- format_file_size(file_size)

    if (!is.na(file_size) && file_size > 1000) {
      result$duckdb$valid <- TRUE
    } else {
      result$duckdb$is_lfs_pointer <- TRUE
    }
  }

  # Test actual connection if requested
  if (test_connection && result$duckdb$valid) {
    tryCatch({
      if (requireNamespace("duckdb", quietly = TRUE) &&
          requireNamespace("DBI", quietly = TRUE)) {
        con <- DBI::dbConnect(duckdb::duckdb(), dbdir = duckdb_path, read_only = TRUE)
        tables <- DBI::dbListTables(con)
        DBI::dbDisconnect(con)
        result$duckdb$can_connect <- TRUE
        result$duckdb$table_count <- length(tables)
      } else {
        result$duckdb$can_connect <- FALSE
        result$duckdb$connection_error <- "duckdb or DBI package not available"
      }
    }, error = function(e) {
      result$duckdb$can_connect <- FALSE
      result$duckdb$connection_error <- e$message
    })
  }

  if (verbose) {
    cat("[DuckDB]\n")
    cat("  Path: ", duckdb_path, "\n")
    cat("  Exists: ", result$duckdb$exists, "\n")
    if (result$duckdb$exists) {
      cat("  Size: ", result$duckdb$size_human, "\n")
      cat("  Valid: ", result$duckdb$valid, "\n")
      if (result$duckdb$is_lfs_pointer) {
        cat("  Warning: Appears to be LFS pointer file\n")
      }
    }
    if (!is.na(result$duckdb$can_connect)) {
      cat("  Connection Test: ", if (result$duckdb$can_connect) "SUCCESS" else "FAILED", "\n")
      if (result$duckdb$can_connect) {
        cat("  Tables: ", result$duckdb$table_count, "\n")
      } else {
        cat("  Error: ", result$duckdb$connection_error, "\n")
      }
    }
    cat("\n")
  }

  # ─────────────────────────────────────────────────────────────────
  # 3. Check Supabase availability
  # ─────────────────────────────────────────────────────────────────
  supabase_host <- Sys.getenv("SUPABASE_DB_HOST", "")
  supabase_password <- Sys.getenv("SUPABASE_DB_PASSWORD", "")
  supabase_port <- Sys.getenv("SUPABASE_DB_PORT", "5432")
  supabase_dbname <- Sys.getenv("SUPABASE_DB_NAME", "postgres")
  supabase_user <- Sys.getenv("SUPABASE_DB_USER", "postgres")

  result$supabase <- list(
    host_set = supabase_host != "",
    host = if (supabase_host != "") mask_string(supabase_host) else "(not set)",
    password_set = supabase_password != "",
    port = supabase_port,
    dbname = supabase_dbname,
    user = supabase_user,
    configured = supabase_host != "" && supabase_password != "",
    can_connect = NA,
    connection_error = NA
  )

  # Test actual connection if requested
  if (test_connection && result$supabase$configured) {
    tryCatch({
      if (requireNamespace("RPostgres", quietly = TRUE) &&
          requireNamespace("DBI", quietly = TRUE)) {
        con <- DBI::dbConnect(
          RPostgres::Postgres(),
          host = supabase_host,
          port = as.integer(supabase_port),
          dbname = supabase_dbname,
          user = supabase_user,
          password = supabase_password,
          sslmode = "require"
        )
        tables <- DBI::dbListTables(con)
        DBI::dbDisconnect(con)
        result$supabase$can_connect <- TRUE
        result$supabase$table_count <- length(tables)
      } else {
        result$supabase$can_connect <- FALSE
        result$supabase$connection_error <- "RPostgres or DBI package not available"
      }
    }, error = function(e) {
      result$supabase$can_connect <- FALSE
      result$supabase$connection_error <- e$message
    })
  }

  if (verbose) {
    cat("[Supabase]\n")
    cat("  SUPABASE_DB_HOST: ", result$supabase$host, "\n")
    cat("  SUPABASE_DB_PASSWORD: ", if (result$supabase$password_set) "(set)" else "(not set)", "\n")
    cat("  SUPABASE_DB_PORT: ", result$supabase$port, "\n")
    cat("  SUPABASE_DB_NAME: ", result$supabase$dbname, "\n")
    cat("  SUPABASE_DB_USER: ", result$supabase$user, "\n")
    cat("  Configured: ", result$supabase$configured, "\n")
    if (!is.na(result$supabase$can_connect)) {
      cat("  Connection Test: ", if (result$supabase$can_connect) "SUCCESS" else "FAILED", "\n")
      if (result$supabase$can_connect) {
        cat("  Tables: ", result$supabase$table_count, "\n")
      } else {
        cat("  Error: ", result$supabase$connection_error, "\n")
      }
    }
    cat("\n")
  }

  # ─────────────────────────────────────────────────────────────────
  # 4. Generate recommendation
  # ─────────────────────────────────────────────────────────────────
  mode <- result$config$mode

  # Mode explanation
  mode_explanations <- list(
    duckdb = "Forced DuckDB mode: Will ONLY use local DuckDB file. Useful for local development.",
    supabase = "Forced Supabase mode: Will ONLY use Supabase PostgreSQL. Useful for testing deployment locally.",
    auto = "Auto mode: Prefers DuckDB if valid file exists, otherwise falls back to Supabase."
  )
  result$mode_explanation <- mode_explanations[[mode]] %||% mode_explanations[["auto"]]

  # Recommendation based on current state
  duckdb_ready <- result$duckdb$valid
  supabase_ready <- result$supabase$configured

  if (mode == "duckdb" && !duckdb_ready) {
    result$recommendation <- paste0(
      "ERROR: DuckDB mode selected but file not available.\n",
      "  Action: Run 'git lfs pull' or change mode to 'auto'"
    )
  } else if (mode == "supabase" && !supabase_ready) {
    result$recommendation <- paste0(
      "ERROR: Supabase mode selected but credentials not configured.\n",
      "  Action: Set SUPABASE_DB_HOST and SUPABASE_DB_PASSWORD env vars"
    )
  } else if (mode == "auto" && !duckdb_ready && !supabase_ready) {
    result$recommendation <- paste0(
      "ERROR: Auto mode but no backend available.\n",
      "  Action: Either run 'git lfs pull' for DuckDB or set Supabase env vars"
    )
  } else if (duckdb_ready && supabase_ready) {
    result$recommendation <- paste0(
      "READY: Both backends available. Current mode '", mode, "' will use ",
      if (mode == "supabase") "Supabase" else "DuckDB", "."
    )
  } else if (duckdb_ready) {
    result$recommendation <- "READY: DuckDB available. Will use DuckDB."
  } else if (supabase_ready) {
    result$recommendation <- "READY: Supabase available. Will use Supabase."
  }

  if (verbose) {
    cat("[Mode]\n")
    cat("  Current: ", mode, "\n")
    cat("  Meaning: ", result$mode_explanation, "\n")
    cat("\n")
    cat("[Recommendation]\n")
    cat("  ", result$recommendation, "\n")
    cat("\n")
  }

  invisible(result)
}

#' Format file size to human readable string
#' @keywords internal
format_file_size <- function(size_bytes) {
  if (is.na(size_bytes)) return("NA")
  if (size_bytes < 1024) return(paste0(size_bytes, " B"))
  if (size_bytes < 1024^2) return(paste0(round(size_bytes / 1024, 1), " KB"))
  if (size_bytes < 1024^3) return(paste0(round(size_bytes / 1024^2, 1), " MB"))
  return(paste0(round(size_bytes / 1024^3, 1), " GB"))
}

#' Mask a string for display (show first and last parts only)
#' @keywords internal
mask_string <- function(s, visible_chars = 4) {
  if (nchar(s) <= visible_chars * 2) return("****")
  paste0(
    substr(s, 1, visible_chars),
    "****",
    substr(s, nchar(s) - visible_chars + 1, nchar(s))
  )
}
